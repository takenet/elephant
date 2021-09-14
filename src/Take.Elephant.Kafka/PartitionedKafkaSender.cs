using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Take.Elephant.Kafka
{
    public class PartitionedKafkaSender<TKey, TEvent> : IEventStreamPublisher<TKey, TEvent>, IDisposable
    {
        private readonly IProducer<TKey, TEvent> _producer;

        public PartitionedKafkaSender(string bootstrapServers, string topic)
            : this(new ProducerConfig() { BootstrapServers = bootstrapServers }, topic)
        {
        }

        public PartitionedKafkaSender(
            ProducerConfig producerConfig,
            string topic)
            : this(
                  new ProducerBuilder<TKey, TEvent>(producerConfig)
                        .Build(),
                  topic)
        {
        }

        public PartitionedKafkaSender(
            IProducer<TKey, TEvent> producer,
            string topic)
        {
            if (string.IsNullOrWhiteSpace(topic))
            {
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(topic));
            }

            _producer = producer;
            Topic = topic;
        }

        public string Topic { get; }

        public Task PublishAsync(TKey key, TEvent item, CancellationToken cancellationToken)
        {
            return _producer.ProduceAsync(
                Topic,
                new Message<TKey, TEvent>
                {
                    Key = key,
                    Value = item
                });
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _producer?.Dispose();
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}