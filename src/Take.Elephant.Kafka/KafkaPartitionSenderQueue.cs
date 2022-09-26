using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Take.Elephant.Kafka
{
    public class KafkaPartitionSenderQueue<T> : IPartitionSenderQueue<T>, IDisposable
    {
        private readonly IEventStreamPublisher<string, string> _producer;
        private readonly ISerializer<T> _serializer;

        public KafkaPartitionSenderQueue(string bootstrapServers, string topic, ISerializer<T> serializer)
        : this(new ProducerConfig() { BootstrapServers = bootstrapServers }, topic, serializer) { }

        public KafkaPartitionSenderQueue(
            string bootstrapServers,
            string topic,
            ISerializer<T> serializer,
            Confluent.Kafka.ISerializer<string> kafkaSerializer)
        : this(new ProducerConfig() { BootstrapServers = bootstrapServers }, topic, serializer, kafkaSerializer) { }

        public KafkaPartitionSenderQueue(
            ProducerConfig producerConfig,
            string topic,
            ISerializer<T> serializer)
        : this(producerConfig, topic, serializer, null) { }

        public KafkaPartitionSenderQueue(
            ProducerConfig producerConfig,
            string topic,
            ISerializer<T> serializer,
            Confluent.Kafka.ISerializer<string> kafkaSerializer)
        {
            Topic = topic;
            _serializer = serializer;
            _producer = new KafkaEventStreamPublisher<string, string>(producerConfig, topic, kafkaSerializer ?? new StringSerializer());
        }

        public KafkaPartitionSenderQueue(
            IProducer<string, string> producer,
            ISerializer<T> serializer,
            string topic)
        {
            Topic = topic;
            _serializer = serializer;
            _producer = new KafkaEventStreamPublisher<string, string>(producer, topic);
        }

        public string Topic { get; }

        public virtual Task EnqueueAsync(T item, string key, CancellationToken cancellationToken = default)
        {
            var stringItem = _serializer.Serialize(item);
            return _producer.PublishAsync(key, stringItem, cancellationToken);
        }

        public void Dispose() => (_producer as IDisposable)?.Dispose();
    }
}
