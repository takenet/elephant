using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Take.Elephant.Kafka
{
    public class KafkaPartitionQueue<T> : IKafkaReceiverQueue<T>, IPartitionSenderQueue<T>, ICloseable, IDisposable
    {
        private readonly KafkaPartitionSenderQueue<T> _senderQueue;
        private readonly KafkaReceiverQueue<T> _receiverQueue;

        public KafkaPartitionQueue(
            ProducerConfig producerConfig,
            ConsumerConfig consumerConfig,
            string topic,
            ISerializer<T> serializer,
            Confluent.Kafka.ISerializer<string> kafkaSerializer = null,
            IDeserializer<string> kafkaDeserializer = null,
            IKafkaHeaderProvider headerProvider = null)
        {
            _senderQueue = new(producerConfig, topic, serializer, kafkaSerializer, headerProvider);
            _receiverQueue = new(consumerConfig, topic, serializer, kafkaDeserializer);
        }

        public Task EnqueueAsync(T item, string key, CancellationToken cancellationToken = default)
        {
            return _senderQueue.EnqueueAsync(item, key, cancellationToken);
        }

        public Task<T> DequeueOrDefaultAsync(CancellationToken cancellationToken = default)
        {
            return _receiverQueue.DequeueOrDefaultAsync(cancellationToken);
        }

        public Task<KafkaConsumedMessage<T>> DequeueWithHeadersOrDefaultAsync(
            CancellationToken cancellationToken = default
        )
        {
            return _receiverQueue.DequeueWithHeadersOrDefaultAsync(cancellationToken);
        }

        public Task<T> DequeueAsync(CancellationToken cancellationToken)
        {
            return _receiverQueue.DequeueAsync(cancellationToken);
        }

        public Task<KafkaConsumedMessage<T>> DequeueWithHeadersAsync(
            CancellationToken cancellationToken
        )
        {
            return _receiverQueue.DequeueWithHeadersAsync(cancellationToken);
        }

        public Task CloseAsync(CancellationToken cancellationToken) => _receiverQueue.CloseAsync(cancellationToken);

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing) return;
            _senderQueue?.Dispose();
            _receiverQueue?.Dispose();
        }
    }
}
