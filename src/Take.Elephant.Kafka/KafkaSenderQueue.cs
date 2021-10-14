using Confluent.Kafka;
using Newtonsoft.Json;
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
            : this(producerConfig, topic, serializer, null)
        {
        }

        public KafkaSenderQueue(
            ProducerConfig producerConfig,
            string topic,
            ISerializer<T> serializer,
            Confluent.Kafka.ISerializer<string> kafkaSerializer)
        {
            Topic = topic;
            _serializer = serializer;
            _producer = new KafkaEventStreamPublisher<Null, string>(producerConfig, topic, kafkaSerializer ?? new StringSerializer());
        }

        public KafkaSenderQueue(
            IProducer<Null, string> producer,
            ISerializer<T> serializer,
            string topic)
        {
            _serializer = serializer;
            Topic = topic;
            _producer = new KafkaEventStreamPublisher<Null, string>(producer, topic);
        }

        public string Topic { get; }

        public virtual Task EnqueueAsync(T item, CancellationToken cancellationToken = default)
        {
            var stringItem = _serializer.Serialize(item);
            return _producer.PublishAsync(null, stringItem, cancellationToken);
        }

        public void Dispose() => (_producer as IDisposable)?.Dispose();
    }

    public class StringSerializer : Confluent.Kafka.ISerializer<string>
    {
        public byte[] Serialize(string data, SerializationContext context)
        {
            return Serializers.Utf8.Serialize(data, context);
        }
    }
}