using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.ServiceBus.Management;

namespace Take.Elephant.Azure
{
    public class AzureServiceBusQueue<T> : IBlockingQueue<T>, ICloseable
    {
        private const int MIN_RECEIVE_TIMEOUT = 250;
        private const int MAX_RECEIVE_TIMEOUT = 30000;

        private readonly string _entityPath;
        private readonly ISerializer<T> _serializer;
        private QueueDescription _queueDescription;
        private readonly ReceiveMode _receiveMode;
        private readonly MessageSender _messageSender;
        private readonly MessageReceiver _messageReceiver;
        private readonly ManagementClient _managementClient;
        private readonly SemaphoreSlim _queueCreationSemaphore;
        private bool _queueExists;

        public AzureServiceBusQueue(
            string connectionString,
            string entityPath,
            ISerializer<T> serializer,
            ReceiveMode receiveMode = ReceiveMode.PeekLock,
            RetryPolicy retryPolicy = null,
            int receiverPrefetchCount = 0,
            QueueDescription queueDescription = null)
        {
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            _entityPath = entityPath;
            _receiveMode = receiveMode;
            _messageSender = new MessageSender(connectionString, entityPath, retryPolicy);
            _messageReceiver = new MessageReceiver(connectionString, entityPath, _receiveMode, retryPolicy, receiverPrefetchCount);
            _managementClient = new ManagementClient(connectionString);
            _queueCreationSemaphore = new SemaphoreSlim(1, 1);

            if (queueDescription != null)
            {
                queueDescription.Path = _entityPath;
                _queueDescription = queueDescription;
            }
            else
            {
                _queueDescription = new QueueDescription(_entityPath);
            }
        }

        public async Task EnqueueAsync(T item, CancellationToken cancellationToken = default)
        {
            await CreateQueueIfNotExistsAsync(cancellationToken);
            var serializedItem = _serializer.Serialize(item);
            await _messageSender.SendAsync(new Message(Encoding.UTF8.GetBytes(serializedItem)));
        }

        public async Task<T> DequeueOrDefaultAsync(CancellationToken cancellationToken = default)
        {
            await CreateQueueIfNotExistsAsync(cancellationToken);

            try
            {
                var message = await _messageReceiver.ReceiveAsync(
                    TimeSpan.FromMilliseconds(MIN_RECEIVE_TIMEOUT));
                if (message != null)
                {
                    return await CreateItemAndCompleteAsync(message);
                }
            }
            catch (ServiceBusTimeoutException) { }

            return default;
        }

        public async Task<T> DequeueAsync(CancellationToken cancellationToken)
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
                    
                    var message = await _messageReceiver.ReceiveAsync(
                        TimeSpan.FromMilliseconds(timeout));
                    if (message != null)
                    {
                        return await CreateItemAndCompleteAsync(message);
                    }
                }
                catch (ServiceBusTimeoutException) { }

                tryCount++;
            }
        }

        public async Task<long> GetLengthAsync(CancellationToken cancellationToken = default)
        {
            await CreateQueueIfNotExistsAsync(cancellationToken);
           
            var queueRuntimeInfo = await _managementClient.GetQueueRuntimeInfoAsync(_path, cancellationToken);
            return queueRuntimeInfo.MessageCount;
        }

        public async Task CloseAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            
            await Task.WhenAll(
                _messageSender.CloseAsync(),
                _messageReceiver.CloseAsync(),
                _managementClient.CloseAsync());
        }

        private async Task CreateQueueIfNotExistsAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            if (_queueExists) return;

            await _queueCreationSemaphore.WaitAsync(cancellationToken);
            try
            {
                if (_queueExists) return;

                _queueExists = await _managementClient.QueueExistsAsync(_entityPath, cancellationToken);

                if (!_queueExists)
                {
                    try
                    {
                        _queueDescription = await _managementClient.CreateQueueAsync(_queueDescription, cancellationToken);
                    }
                    catch (MessagingEntityAlreadyExistsException)
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

        private async Task<T> CreateItemAndCompleteAsync(Message message)
        {
            var serializedItem = Encoding.UTF8.GetString(message.Body);
            var item = _serializer.Deserialize(serializedItem);
            if (_receiveMode == ReceiveMode.PeekLock)
            {
                await _messageReceiver.CompleteAsync(message.SystemProperties.LockToken);
            }
            return item;
        }
    }
}
