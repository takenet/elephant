using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Producer;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Azure
{
    public class AzureEventHubQueue<T> : IBlockingQueue<T>, IBatchSenderQueue<T>, IAsyncDisposable
    {
        private readonly AzureEventHubReceiverQueue<T> _receiver;
        private readonly AzureEventHubSenderQueue<T> _sender;

        public AzureEventHubQueue(string connectionString, string topic, string consumerGroup, ISerializer<T> serializer)
        {
            _receiver = new AzureEventHubReceiverQueue<T>(connectionString, topic, consumerGroup, serializer);
            _sender = new AzureEventHubSenderQueue<T>(connectionString, topic, serializer);
        }

        public AzureEventHubQueue(EventHubConsumerClient consumer, EventHubProducerClient producer, ISerializer<T> serializer)
        {
            _receiver = new AzureEventHubReceiverQueue<T>(consumer, serializer);
            _sender = new AzureEventHubSenderQueue<T>(producer, serializer);
        }

        public EventHandler<ExceptionEventArgs> ConsumerFailed
        {
            set => _receiver.ConsumerFailed += value;
        }

        public Task<T> DequeueAsync(CancellationToken cancellationToken)
        {
            return _receiver.DequeueAsync(cancellationToken);
        }

        public Task<T> DequeueOrDefaultAsync(CancellationToken cancellationToken = default)
        {
            return _receiver.DequeueOrDefaultAsync(cancellationToken);
        }

        public Task EnqueueAsync(T item, CancellationToken cancellationToken = default)
        {
            return _sender.EnqueueAsync(item, cancellationToken);
        }

        public Task EnqueueBatchAsync(IEnumerable<T> items, CancellationToken cancellationToken = default)
        {
            return _sender.EnqueueBatchAsync(items, cancellationToken);
        }

        public Task<long> GetLengthAsync(CancellationToken cancellationToken = default)
        {
            return Task.FromException<long>(
               new NotSupportedException(
                   "It is not possible to determine the number of unhandled messages on a event hub topic"));
        }

        public virtual async ValueTask DisposeAsync()
        {
            await _sender.DisposeAsync();
            await _receiver.DisposeAsync();
        }
    }
}