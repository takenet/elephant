using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Kafka
{
    public class KafkaSenderQueue<T> : ISenderQueue<T>, IDisposable, IStreamSenderQueue<T>
    {
        private readonly IProducer<Null, string> _producer;
        private readonly IProducer<string, string> _producerKey;
        private readonly ISerializer<T> _serializer;

        public KafkaSenderQueue(string bootstrapServers, string topic, ISerializer<T> serializer, Confluent.Kafka.ISerializer<string> kafkaSerializer = null)
            : this(new ProducerConfig() { BootstrapServers = bootstrapServers }, topic, serializer, kafkaSerializer)
        {
        }

        public KafkaSenderQueue(
            ProducerConfig producerConfig,
            string topic,
            ISerializer<T> serializer,
            Confluent.Kafka.ISerializer<string> kafkaSerializer = null)
            : this(
                  new ProducerBuilder<Null, string>(producerConfig)
                        .SetValueSerializer(kafkaSerializer ?? new StringSerializer())
                        .Build(),
                  serializer,
                  topic)
        {
        }

        public KafkaSenderQueue(
            IProducer<Null, string> producer,
            ISerializer<T> serializer,
            string topic)
        {
            if (string.IsNullOrWhiteSpace(topic))
            {
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(topic));
            }

            _producer = producer;
            _serializer = serializer;
            Topic = topic;
        }

        public KafkaSenderQueue(
            IProducer<string, string> producerKey,
            ISerializer<T> serializer,
            string topic)
        {
            if (string.IsNullOrWhiteSpace(topic))
            {
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(topic));
            }

            _producerKey = producerKey;
            _serializer = serializer;
            Topic = topic;
        }

        public string Topic { get; }

        public virtual Task EnqueueAsync(T item, CancellationToken cancellationToken = default)
        {
            var stringItem = _serializer.Serialize(item);
            return _producer.ProduceAsync(
                Topic,
                new Message<Null, string>
                {
                    Value = stringItem
                });
        }

        public virtual Task EnqueueAsync(T item, string messageId, CancellationToken cancellationToken = default)
        {
            var stringItem = _serializer.Serialize(item);
            return _producerKey.ProduceAsync(
                Topic,
                new Message<string, string>
                {
                    Key = messageId,
                    Value = stringItem
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

    public class JsonSerializer<T> : Confluent.Kafka.ISerializer<T>
    {
        public byte[] Serialize(T data, SerializationContext context)
        {
            var json = JsonConvert.SerializeObject(data, Formatting.None);

            return Serializers.Utf8.Serialize(json, context);
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