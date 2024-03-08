using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

namespace Take.Elephant.Azure
{
    public class AzureServiceBusQueue<T> : IBlockingQueue<T>, IBatchSenderQueue<T>, ICloseable
    {
        private const int MIN_RECEIVE_TIMEOUT = 250;
        private const int MAX_RECEIVE_TIMEOUT = 30000;

        private readonly string _entityPath;
        private readonly ISerializer<T> _serializer;
        private CreateQueueOptions _queueOptions;
        private readonly ServiceBusReceiveMode _receiveMode;
        private readonly ServiceBusClient _client;
        private readonly ServiceBusSender _messageSender;
        private readonly ServiceBusReceiver _messageReceiver;
        private readonly ServiceBusAdministrationClient _administrationClient;
        private readonly SemaphoreSlim _queueCreationSemaphore;
        private bool _queueExists;

        public AzureServiceBusQueue(
            string connectionString,
            string entityPath,
            ISerializer<T> serializer,
            ServiceBusReceiveMode receiveMode = ServiceBusReceiveMode.PeekLock,
            ServiceBusRetryOptions retryOptions = null,
            int receiverPrefetchCount = 0,
            CreateQueueOptions queueOptions = null)
        {
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            _entityPath = entityPath;
            _receiveMode = receiveMode;
            _client = new ServiceBusClient(connectionString, new ServiceBusClientOptions
            {
                RetryOptions = retryOptions ?? new ServiceBusRetryOptions(),
            });
            _messageSender = _client.CreateSender(entityPath);
            _messageReceiver = _client.CreateReceiver(entityPath, new ServiceBusReceiverOptions
            {
                ReceiveMode = _receiveMode,
                PrefetchCount = receiverPrefetchCount
            });
            _administrationClient = new ServiceBusAdministrationClient(connectionString);
            _queueCreationSemaphore = new SemaphoreSlim(1, 1);

            if (queueOptions != null)
            {
                queueOptions.Name = _entityPath;
                _queueOptions = queueOptions;
            }
            else
            {
                _queueOptions = new CreateQueueOptions(_entityPath);
            }
        }

        public virtual async Task EnqueueAsync(T item, CancellationToken cancellationToken = default)
        {
            await CreateQueueIfNotExistsAsync(cancellationToken);
            var message = CreateMessage(item);
            await _messageSender.SendMessageAsync(message, cancellationToken);
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
            
            await _messageSender.SendMessagesAsync(batch, cancellationToken);
        }

        public virtual async Task<T> DequeueOrDefaultAsync(CancellationToken cancellationToken = default)
        {
            await CreateQueueIfNotExistsAsync(cancellationToken);

            try
            {
                var message = await _messageReceiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(MIN_RECEIVE_TIMEOUT), cancellationToken);
                if (message != null)
                {
                    return await CreateItemAndCompleteAsync(message, cancellationToken);
                }
            }
            catch (ServiceBusException ex) when (ex.Reason == ServiceBusFailureReason.ServiceTimeout) { }

            return default;
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
                    {
                        timeout = MAX_RECEIVE_TIMEOUT;
                    }
                    
                    var message = await _messageReceiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(timeout), cancellationToken);
                    if (message != null)
                    {
                        return await CreateItemAndCompleteAsync(message, cancellationToken);
                    }
                }
                catch (ServiceBusException ex) when (ex.Reason == ServiceBusFailureReason.ServiceTimeout) { }

                tryCount++;
            }
        }

        public virtual async Task<long> GetLengthAsync(CancellationToken cancellationToken = default)
        {
            await CreateQueueIfNotExistsAsync(cancellationToken);
            
            var queueRuntimeProperties = await _administrationClient.GetQueueRuntimePropertiesAsync(_entityPath, cancellationToken);
            return queueRuntimeProperties.Value.TotalMessageCount;
        }

        public virtual async Task CloseAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            
            await Task.WhenAll(
                _messageSender.CloseAsync(),
                _messageReceiver.CloseAsync());
        }

        private async Task CreateQueueIfNotExistsAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            if (_queueExists) return;

            await _queueCreationSemaphore.WaitAsync(cancellationToken);
            try
            {
                if (_queueExists) return;

                _queueExists = await _administrationClient.QueueExistsAsync(_entityPath, cancellationToken);

                if (!_queueExists)
                {
                    try
                    {
                        await _administrationClient.CreateQueueAsync(_queueOptions, cancellationToken);
                    }
                    catch (ServiceBusException ex) when (ex.Reason == ServiceBusFailureReason.MessagingEntityAlreadyExists)
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

        private async Task<T> CreateItemAndCompleteAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken)
        {
            var serializedItem = Encoding.UTF8.GetString(message.Body);
            var item = _serializer.Deserialize(serializedItem);
            if (_receiveMode == ServiceBusReceiveMode.PeekLock)
            {
                await _messageReceiver.CompleteMessageAsync(message, cancellationToken);
            }
            return item;
        }
    }
}
