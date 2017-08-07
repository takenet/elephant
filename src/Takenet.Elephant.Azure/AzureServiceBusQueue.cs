using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;

namespace Takenet.Elephant.Azure
{
    public class AzureServiceBusQueue<T> : IBlockingQueue<T>, ICloseable
    {
        private readonly string _connectionString;
        private readonly string _path;
        private readonly ISerializer<T> _serializer;
        private readonly NamespaceManager _namespaceManager;
        private readonly TimeSpan _dequeueTimeout;
        private readonly SemaphoreSlim _queueCreationSemaphore;
        private readonly ConcurrentQueue<Task<BrokeredMessage>> _pendingReceiveTasks;

        private QueueClient _queueClient;

        public AzureServiceBusQueue(
            string connectionString, 
            string path, 
            ISerializer<T> serializer,
            TimeSpan? dequeueTimeout = null)
        {
            _connectionString = connectionString;
            _path = path;
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));            
            _namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);
            _dequeueTimeout = dequeueTimeout ?? TimeSpan.FromMilliseconds(500);
            _queueCreationSemaphore = new SemaphoreSlim(1);
            _pendingReceiveTasks = new ConcurrentQueue<Task<BrokeredMessage>>();
        }

        public async Task EnqueueAsync(T item)
        {
            var queueClient = await GetQueueClientAsync().ConfigureAwait(false);
            var serializedItem = _serializer.Serialize(item);
            await queueClient.SendAsync(new BrokeredMessage(serializedItem)).ConfigureAwait(false);
        }

        public async Task<T> DequeueOrDefaultAsync()
        {
            var queueClient = await GetQueueClientAsync().ConfigureAwait(false);
            var message = await queueClient.ReceiveAsync(_dequeueTimeout).ConfigureAwait(false);
            if (message == null) return default(T);
            return CreateItem(message);            
        }

        public async Task<long> GetLengthAsync()
        {
            var queueDescription = await GetOrCreateQueueDescriptionAsync().ConfigureAwait(false);
            return queueDescription.MessageCount;
        }

        public async Task<T> DequeueAsync(CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<BrokeredMessage>();
            using (cancellationToken.Register(() => tcs.TrySetCanceled(cancellationToken)))
            {                
                while (!cancellationToken.IsCancellationRequested)
                {
                    var queueClient = await GetQueueClientAsync().ConfigureAwait(false);

                    Task<BrokeredMessage> receiveFromQueueTask;
                    if (!_pendingReceiveTasks.TryDequeue(out receiveFromQueueTask))
                    {
                        receiveFromQueueTask = queueClient.ReceiveAsync();
                    }

                    try
                    {
                        var completedTask = await
                            Task.WhenAny(receiveFromQueueTask, tcs.Task).ConfigureAwait(false);

                        var message = await completedTask.ConfigureAwait(false);
                        if (message != null) return CreateItem(message);
                    }
                    catch (TaskCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        _pendingReceiveTasks.Enqueue(receiveFromQueueTask);
                        throw;
                    }
                }
            }

            cancellationToken.ThrowIfCancellationRequested();
            throw new InvalidOperationException("Something very wrong happened");
        }

        public Task CloseAsync(CancellationToken cancellationToken) 
            => _queueClient != null 
                ? _queueClient.CloseAsync()
                : Task.CompletedTask;

        private async Task<QueueClient> GetQueueClientAsync()
        {
            if (_queueClient != null) return _queueClient;

            using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30)))
            {

                await _queueCreationSemaphore.WaitAsync(cts.Token).ConfigureAwait(false);
                try
                {
                    if (_queueClient != null) return _queueClient;
                    // Force the creation of the queue, if it does not exists
                    await GetOrCreateQueueDescriptionAsync().ConfigureAwait(false);
                    _queueClient = QueueClient.CreateFromConnectionString(_connectionString, _path, ReceiveMode.ReceiveAndDelete);
                }
                finally
                {
                    _queueCreationSemaphore.Release();
                }
            }

            return _queueClient;
        }

        private async Task<QueueDescription> GetOrCreateQueueDescriptionAsync()
        {
            try
            {
                return await _namespaceManager.GetQueueAsync(_path).ConfigureAwait(false);
            }
            catch (MessagingEntityNotFoundException)
            {
                return await _namespaceManager.CreateQueueAsync(_path).ConfigureAwait(false);
            }
        }
        private T CreateItem(BrokeredMessage message) => _serializer.Deserialize(message.GetBody<string>());
    }
}
