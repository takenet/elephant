using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Kafka
{
    public class KafkaSenderQueue<T> : ISenderQueue<T>, IDisposable
    {
        private readonly IProducer<Null, T> _producer;
        private readonly string _topic;
        private readonly Func<T, string> _keyFactory;

        public KafkaSenderQueue(string bootstrapServers, string topic, Confluent.Kafka.ISerializer<T> serializer)
            : this(new ProducerConfig() { BootstrapServers = bootstrapServers }, topic, serializer)
        {
        }

        public KafkaSenderQueue(
            ProducerConfig producerConfig,
            string topic,
            Confluent.Kafka.ISerializer<T> serializer)
        {
            if (string.IsNullOrWhiteSpace(topic))
            {
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(topic));
            }

            _producer = new ProducerBuilder<Null, T>(producerConfig)
                .SetValueSerializer(serializer)
                .Build();

            _topic = topic;
        }

        public KafkaSenderQueue(
            IProducer<Null, T> producer,
            string topic)
        {
            if (string.IsNullOrWhiteSpace(topic))
            {
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(topic));
            }

            _producer = producer;
            _topic = topic;
        }

        public virtual Task EnqueueAsync(T item, CancellationToken cancellationToken = default)
        {
            return _producer.ProduceAsync(
                _topic,
                new Message<Null, T>
                {
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

    public class JsonSerializer<T> : Confluent.Kafka.ISerializer<T>
    {
        public byte[] Serialize(T data, SerializationContext context)
        {
            var json = JsonConvert.SerializeObject(data, Formatting.None);

            return Serializers.Utf8.Serialize(json, context);
        }
    }
}