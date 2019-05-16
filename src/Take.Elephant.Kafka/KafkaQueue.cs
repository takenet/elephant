using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Kafka
{
    public class KafkaQueue<T> : IBlockingQueue<T>, ICloseable, IDisposable
    {
        private readonly KafkaSenderQueue<T> _senderQueue;
        private readonly KafkaReceiverQueue<T> _receiverQueue;

        public KafkaQueue(
            ProducerConfig producerConfig,
            ConsumerConfig consumerConfig,
            string topic,
            Confluent.Kafka.ISerializer<T> serializer = null,
            IDeserializer<T> deserializer = null)
        {
            _senderQueue = new KafkaSenderQueue<T>(producerConfig, topic, serializer);
            _receiverQueue = new KafkaReceiverQueue<T>(consumerConfig, topic, deserializer);
        }

        public virtual Task EnqueueAsync(T item, CancellationToken cancellationToken = default)
        {
            return _senderQueue.EnqueueAsync(item, cancellationToken);
        }

        public virtual Task<T> DequeueAsync(CancellationToken cancellationToken)
        {
            return _receiverQueue.DequeueAsync(cancellationToken);
        }

        public virtual Task<T> DequeueOrDefaultAsync(CancellationToken cancellationToken = default)
        {
            return _receiverQueue.DequeueOrDefaultAsync(cancellationToken);
        }

        public virtual Task CloseAsync(CancellationToken cancellationToken)
        {
            return _receiverQueue.CloseAsync(cancellationToken);
        }

        public virtual Task<long> GetLengthAsync(CancellationToken cancellationToken = default)
        {
            return Task.FromException<long>(
                new NotSupportedException(
                    "It is not possible to determine the number of unhandled messages on a Kafka topic"));
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _senderQueue?.Dispose();
                _receiverQueue?.Dispose();
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}