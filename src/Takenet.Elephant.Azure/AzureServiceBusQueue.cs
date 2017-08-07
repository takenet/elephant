using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;

namespace Takenet.Elephant.Azure
{
    public class AzureServiceBusQueue<T> : IBlockingQueue<T>, ICloseable
    {
        private readonly ISerializer<T> _serializer;
        private readonly QueueClient _queueClient;
        private readonly NamespaceManager _namespaceManager;
        private readonly TimeSpan _dequeueTimeout;

        public AzureServiceBusQueue(
            string connectionString, 
            string path, 
            ISerializer<T> serializer,
            TimeSpan? dequeueTimeout = null)
        {            
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            _queueClient = QueueClient.CreateFromConnectionString(connectionString, path, ReceiveMode.ReceiveAndDelete);            
            _namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);
            _dequeueTimeout = dequeueTimeout ?? TimeSpan.FromMilliseconds(500);
        }

        public Task EnqueueAsync(T item)
        {
            var serializedItem = _serializer.Serialize(item);
            return _queueClient.SendAsync(new BrokeredMessage(serializedItem));
        }

        public async Task<T> DequeueOrDefaultAsync()
        {            
            var message = await _queueClient.ReceiveAsync(_dequeueTimeout).ConfigureAwait(false);
            if (message == null) return default(T);
            return CreateItem(message);            
        }

        public async Task<long> GetLengthAsync()
        {
            var queueDescription = await _namespaceManager.GetQueueAsync(_queueClient.Path).ConfigureAwait(false);
            return queueDescription.MessageCount;
        }

        public async Task<T> DequeueAsync(CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<BrokeredMessage>();
            using (cancellationToken.Register(() => tcs.TrySetCanceled(cancellationToken)))
            {
                var completedTask = await 
                    Task.WhenAny(_queueClient.ReceiveAsync(), tcs.Task).ConfigureAwait(false);
                var message = await completedTask.ConfigureAwait(false);
                return CreateItem(message);
            }
        }

        public Task CloseAsync(CancellationToken cancellationToken) => _queueClient.CloseAsync();

        private T CreateItem(BrokeredMessage message) => _serializer.Deserialize(message.GetBody<string>());
    }
}
