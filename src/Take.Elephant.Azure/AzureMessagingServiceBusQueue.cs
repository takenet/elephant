using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Azure.ServiceBus;
using ServiceBusException = Azure.Messaging.ServiceBus.ServiceBusException;

namespace Take.Elephant.Azure
{
    public class AzureMessagingServiceBusQueue<T> : IBlockingQueue<T>, IBatchSenderQueue<T>, ICloseable
    {
        private const int MIN_RECEIVE_TIMEOUT = 250;
        private const int MAX_RECEIVE_TIMEOUT = 30000;
        private QueueProperties _queueProperties;
        private readonly string _entityPath;
        private readonly ISerializer<T> _serializer;

        /// <ServiceBusReceiveMode cref="ServiceBusReceiveMode">Check documentation at https://learn.microsoft.com/en-us/dotnet/api/azure.messaging.servicebus.servicebusreceivemode?view=azure-dotnet </ServiceBusReceiveMode>
        private readonly ServiceBusReceiveMode _receiveMode;

        /// <ServiceBusSender cref="ServiceBusSender">Check documentation at https://docs.microsoft.com/en-us/dotnet/api/azure.messaging.servicebus.servicebussender?view=azure-dotnet </ServiceBusSender>
        private readonly ServiceBusSender _messageSender;

        /// <ServiceBusReceiver cref="ServiceBusReceiver">Check documentation at https://docs.microsoft.com/en-us/dotnet/api/azure.messaging.servicebus.servicebusreceiver?view=azure-dotnet </ServiceBusReceiver>
        private ServiceBusReceiver _messageReceiver;

        /// <ServiceBusAdministrationClient cref="ServiceBusAdministrationClient">Check documentation at https://docs.microsoft.com/en-us/dotnet/api/azure.messaging.servicebus.administration.servicebusadministrationclient?view=azure-dotnet </ServiceBusAdministrationClient>
        //substituído private readonly ManagementClient _managementClient por ServiceBusAdministrationClient;
        ///<remarks>The ServiceBusAdministrationClient operates against an entity management endpoint without performance guarantees. It is not recommended for use in performance-critical scenarios.
        ///Check documentation at https://docs.microsoft.com/en-us/dotnet/api/azure.messaging.servicebus.administration.servicebusadministrationclient?view=azure-dotnet </remarks>
        private readonly ServiceBusAdministrationClient _administrationClient;

        /// <ServiceBusClient cref="SemaphoreSlim">Check documentation at https://learn.microsoft.com/en-us/dotnet/api/azure.messaging.servicebus?view=azure-dotnet </ServiceBusClient>
        private readonly ServiceBusClient _serviceBusClient;

        private readonly SemaphoreSlim _queueCreationSemaphore;
        private bool _queueExists;

        public AzureMessagingServiceBusQueue(string connectionString,
            string entityPath,
            ISerializer<T> serializer,
            ServiceBusReceiveMode receiveMode = ServiceBusReceiveMode.PeekLock,
            RetryPolicy retryPolicy = null,
            int receiverPrefetchCount = 0)
        {
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            _entityPath = entityPath;
            _receiveMode = receiveMode;
            _administrationClient = new ServiceBusAdministrationClient(connectionString);
            _queueCreationSemaphore = new SemaphoreSlim(1, 1);
        }

        private async Task CreateQueueIfNotExistsAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            if (_queueExists)
                return;

            await _queueCreationSemaphore.WaitAsync(cancellationToken);
            try
            {
                if (_queueExists)
                    return;

                _queueExists = await _administrationClient.QueueExistsAsync(_entityPath, cancellationToken);

                if (!_queueExists)
                {
                    try
                    {
                        //método substituido _queueDescription = await _managementClient.CreateQueueAsync(_queueDescription, cancellationToken);
                        _queueProperties = await _administrationClient.CreateQueueAsync(_entityPath, cancellationToken: cancellationToken);

                    }
                    catch (ServiceBusException)
                    {
                        // Concurrency creation handling
                    }

                    _queueExists = true;
                }
            }
            finally
            {
                _queueCreationSemaphore.Release();
            }
        }

        private ServiceBusMessage CreateMessage(T item)
        {
            var serializedItem = _serializer.Serialize(item);
            return new ServiceBusMessage(Encoding.UTF8.GetBytes(serializedItem));
        }

        private async Task<T> CreateItemAndCompleteAsync(ServiceBusReceivedMessage message)
        {
            var serializedItem = Encoding.UTF8.GetString(message.Body);
            var item = _serializer.Deserialize(serializedItem);
            if (_receiveMode == ServiceBusReceiveMode.PeekLock)
            {
                //await _messageReceiver.CompleteAsync(message.SystemProperties.LockToken) substituido pelo método abaixo;
                await _messageReceiver.CompleteMessageAsync(message, cancellationToken: default);
            }
            return item;
        }

        public virtual async Task EnqueueAsync(T item, CancellationToken cancellationToken = default)
        {
            await CreateQueueIfNotExistsAsync(cancellationToken);
            var message = CreateMessage(item);
            await _messageSender.SendMessageAsync(message);
        }

        public virtual async Task<T> DequeueAsync(CancellationToken cancellationToken)
        {
            await CreateQueueIfNotExistsAsync(cancellationToken);

            var tryCount = 0;

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    var timeout = MIN_RECEIVE_TIMEOUT * Math.Pow(2, tryCount);

                    if (timeout > MAX_RECEIVE_TIMEOUT)
                        timeout = MAX_RECEIVE_TIMEOUT;

                    var message = await _messageReceiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(timeout));

                    if (message != null)
                        return await CreateItemAndCompleteAsync(message);
                }
                catch (Exception)
                {
                    //TODO: Verificar. Mantive uma exception genérica pois o método ServiceBusReceiver.ReceiveMessageAsync não trata um erro específico.
                }

                tryCount++;
            }
        }

        public virtual async Task<T> DequeueOrDefaultAsync(CancellationToken cancellationToken = default)
        {
            await CreateQueueIfNotExistsAsync(cancellationToken);

            try
            {
                var message = await _messageReceiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(MIN_RECEIVE_TIMEOUT));
                if (message != null)
                    return await CreateItemAndCompleteAsync(message);
            }
            catch (Exception)
            {
                //TODO: Verificar. Substituído ServiceBusTimeoutException por Exception pois o método ServiceBusReceiver.ReceiveMessageAsync não trata um erro específico e retorna null se nenhuma mensagem for encontrada.
            }

            return default;
        }

        /// <QueueRuntimeProperties cref="QueueRuntimeProperties">check documentation https://learn.microsoft.com/en-us/dotnet/api/azure.messaging.servicebus.servicebusmodelfactory.queueruntimeproperties?view=azure-dotnet </QueueRuntimeProperties>
        public virtual async Task<long> GetLengthAsync(CancellationToken cancellationToken = default)
        {
            await CreateQueueIfNotExistsAsync(cancellationToken);

            var queueRuntimeProperties = await _administrationClient.GetQueueRuntimePropertiesAsync(_entityPath, cancellationToken);
            return queueRuntimeProperties.Value.TotalMessageCount;
        }

        public virtual async Task EnqueueBatchAsync(IEnumerable<T> items, CancellationToken cancellationToken = default)
        {
            await CreateQueueIfNotExistsAsync(cancellationToken);
            var batch = new List<ServiceBusMessage>();

            foreach (var item in items)
            {
                var message = CreateMessage(item);
                batch.Add(message);
            }

            //await _messageSender.SendAsync(batch); substituído pelo método abaixo;
            //check documentation https://docs.microsoft.com/en-us/dotnet/api/azure.messaging.servicebus.servicebussender.sendmessagesasync?view=azure-dotnet
            await _messageSender.SendMessagesAsync(batch);
        }

        public virtual async Task CloseAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            await Task.WhenAll(
                _messageSender.CloseAsync(),
                _messageReceiver.CloseAsync());
            //método _managementClient.CloseAsync() removido pois _administrationClient não possui CloseAsync();
        }
    }
}
