using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Kafka
{
    public class KafkaSenderQueue<T> : ISenderQueue<T>, IDisposable
    {
        private readonly IEventStreamPublisher<Null, string> _producer;
        private readonly ISerializer<T> _serializer;

        public KafkaSenderQueue(string bootstrapServers, string topic, ISerializer<T> serializer)
            : this(new ProducerConfig() { BootstrapServers = bootstrapServers }, topic, serializer)
        {
        }

        public KafkaSenderQueue(string bootstrapServers, string topic, ISerializer<T> serializer, Confluent.Kafka.ISerializer<string> kafkaSerializer)
            : this(new ProducerConfig() { BootstrapServers = bootstrapServers }, topic, serializer, kafkaSerializer)
        {
        }

        public KafkaSenderQueue(
            ProducerConfig producerConfig,
            string topic,
            ISerializer<T> serializer)
            : this(producerConfig, topic, serializer, null, null)
        {
        }

        public KafkaSenderQueue(
            ProducerConfig producerConfig,
            string topic,
            ISerializer<T> serializer,
            Confluent.Kafka.ISerializer<string> kafkaSerializer)
            : this(producerConfig, topic, serializer, kafkaSerializer, null)
        {
        }

        public KafkaSenderQueue(
            ProducerConfig producerConfig,
            string topic,
            ISerializer<T> serializer,
            Confluent.Kafka.ISerializer<string> kafkaSerializer,
            IKafkaHeaderProvider headerProvider)
        {
            if (producerConfig == null) throw new ArgumentNullException(nameof(producerConfig));
            Topic = topic ?? throw new ArgumentNullException(nameof(topic));
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            _producer = new KafkaEventStreamPublisher<Null, string>(producerConfig, topic, kafkaSerializer ?? new StringSerializer(), headerProvider);
        }

        public KafkaSenderQueue(
            IProducer<Null, string> producer,
            ISerializer<T> serializer,
            string topic)
        {
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            Topic = topic ?? throw new ArgumentNullException(nameof(topic));
            _producer = new KafkaEventStreamPublisher<Null, string>(producer ?? throw new ArgumentNullException(nameof(producer)), topic, null);
        }

        public KafkaSenderQueue(
            IProducer<Null, string> producer,
            ISerializer<T> serializer,
            string topic,
            IKafkaHeaderProvider headerProvider)
        {
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            Topic = topic ?? throw new ArgumentNullException(nameof(topic));
            _producer = new KafkaEventStreamPublisher<Null, string>(producer ?? throw new ArgumentNullException(nameof(producer)), topic, headerProvider);
        }

        public KafkaSenderQueue(
            IEventStreamPublisher<Null, string> producer,
            ISerializer<T> serializer,
            string topic)
        {
            _producer = producer ?? throw new ArgumentNullException(nameof(producer));
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            Topic = topic ?? throw new ArgumentNullException(nameof(topic));
        }

        public string Topic { get; }

        public virtual Task EnqueueAsync(T item, CancellationToken cancellationToken = default)
        {
            var stringItem = _serializer.Serialize(item);
            return _producer.PublishAsync(null, stringItem, cancellationToken);
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

    public class StringSerializer : Confluent.Kafka.ISerializer<string>
    {
        public byte[] Serialize(string data, SerializationContext context)
        {
            return Serializers.Utf8.Serialize(data, context);
        }
    }
}
