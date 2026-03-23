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
        : this(producerConfig, topic, serializer, null, null) { }

        public KafkaPartitionSenderQueue(
            ProducerConfig producerConfig,
            string topic,
            ISerializer<T> serializer,
            Confluent.Kafka.ISerializer<string> kafkaSerializer)
        : this(producerConfig, topic, serializer, kafkaSerializer, null) { }

        public KafkaPartitionSenderQueue(
            ProducerConfig producerConfig,
            string topic,
            ISerializer<T> serializer,
            Confluent.Kafka.ISerializer<string> kafkaSerializer,
            IKafkaHeaderProvider headerProvider)
        {
            ArgumentNullException.ThrowIfNull(serializer);
            Topic = topic;
            _serializer = serializer;
            _producer = new KafkaEventStreamPublisher<string, string>(producerConfig, topic, kafkaSerializer ?? new StringSerializer(), headerProvider);
        }

        public KafkaPartitionSenderQueue(
            IProducer<string, string> producer,
            ISerializer<T> serializer,
            string topic)
        {
            ArgumentNullException.ThrowIfNull(serializer);
            Topic = topic;
            _serializer = serializer;
            _producer = new KafkaEventStreamPublisher<string, string>(producer, topic, null);
        }

        public KafkaPartitionSenderQueue(
            IProducer<string, string> producer,
            ISerializer<T> serializer,
            string topic,
            IKafkaHeaderProvider headerProvider)
        {
            ArgumentNullException.ThrowIfNull(serializer);
            Topic = topic;
            _serializer = serializer;
            _producer = new KafkaEventStreamPublisher<string, string>(producer, topic, headerProvider);
        }

        public string Topic { get; }

        public virtual Task EnqueueAsync(T item, string key, CancellationToken cancellationToken = default)
        {
            var stringItem = _serializer.Serialize(item);
            return _producer.PublishAsync(key, stringItem, cancellationToken);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing) return;
            (_producer as IDisposable)?.Dispose();
        }
    }
}
