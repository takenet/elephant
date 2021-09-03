using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Kafka
{
    public class KafkaSenderQueue<T> : IStreamSenderQueue<T>, IDisposable
    {
        private readonly IProducer<string, string> _producer;
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
                  new ProducerBuilder<string, string>(producerConfig)
                        .SetValueSerializer(kafkaSerializer ?? new StringSerializer())
                        .Build(),
                  serializer,
                  topic)
        {
        }

        public KafkaSenderQueue(
            IProducer<string, string> producer,
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

        public string Topic { get; }

        public virtual Task EnqueueAsync(T item, CancellationToken cancellationToken = default)
        {
            var stringItem = _serializer.Serialize(item);
            return _producer.ProduceAsync(
                Topic,
                new Message<string, string>
                {
                    Key = Guid.NewGuid().ToString(),
                    Value = stringItem
                });
        }

        public virtual Task EnqueueAsync(T item, string messageId, CancellationToken cancellationToken = default)
        {
            var stringItem = _serializer.Serialize(item);
            return _producer.ProduceAsync(
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