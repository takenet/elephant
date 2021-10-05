using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Take.Elephant.Kafka
{
    public class PartitionedKafkaSender<TKey, TEvent> : IEventStreamPublisher<TKey, TEvent>, IDisposable
    {
        private readonly IProducer<TKey, TEvent> _producer;

        public PartitionedKafkaSender(string bootstrapServers, string topic, ISerializer<TEvent> serializer)
            : this(new ProducerConfig() { BootstrapServers = bootstrapServers }, topic, serializer)
        {
        }

        public PartitionedKafkaSender(string bootstrapServers, string topic, Confluent.Kafka.ISerializer<TEvent> kafkaSerializer)
            : this(new ProducerConfig() { BootstrapServers = bootstrapServers }, topic, kafkaSerializer)
        {
        }

        public PartitionedKafkaSender(
            ProducerConfig producerConfig,            
            string topic,
            ISerializer<TEvent> serializer)
            : this(
                  new ProducerBuilder<TKey, TEvent>(producerConfig)
                        .SetValueSerializer(new EventSerializer(serializer))
                        .Build(),
                  topic)
        {
        }

        public PartitionedKafkaSender(
            ProducerConfig producerConfig,
            string topic,
            Confluent.Kafka.ISerializer<TEvent> kafkaSerializer)
            : this(
                  new ProducerBuilder<TKey, TEvent>(producerConfig)
                        .SetValueSerializer(kafkaSerializer)
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
        public class EventSerializer : Confluent.Kafka.ISerializer<TEvent>
        {
            private readonly ISerializer<TEvent> _serializer;

            public EventSerializer(ISerializer<TEvent> serializer)
            {
                _serializer = serializer;
            }
            public byte[] Serialize(TEvent data, SerializationContext context)
            {
                return Encoding.UTF8.GetBytes(_serializer.Serialize(data));
            }
        }
    }
}