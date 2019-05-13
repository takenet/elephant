using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Newtonsoft.Json;

namespace Take.Elephant.Kafka
{
    public class KafkaSenderQueue<T> : ISenderQueue<T>, IDisposable
    {
        private readonly IProducer<string, T> _producer;
        private readonly string _topic;
        private readonly Func<T, string> _keyFactory;

        public KafkaSenderQueue(string bootstrapServers, string topic, Confluent.Kafka.ISerializer<T> serializer)
            : this(new ProducerConfig() { BootstrapServers = bootstrapServers }, topic, serializer)
        {
            
        }
        
        public KafkaSenderQueue(
            ProducerConfig producerConfig,
            string topic,
            Confluent.Kafka.ISerializer<T> serializer,
            Func<T, string> keyFactory = null)
        {
            if (string.IsNullOrWhiteSpace(topic))
            {
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(topic));
            }
            
            _producer = new ProducerBuilder<string, T>(producerConfig)
                .SetKeySerializer(Serializers.Utf8)
                .SetValueSerializer(serializer)
                .Build();
            
            _topic = topic;
            _keyFactory = keyFactory ?? (i => null);
        }
        
        public virtual Task EnqueueAsync(T item, CancellationToken cancellationToken = default)
        {                
            return _producer.ProduceAsync(
                _topic, 
                new Message<string, T>
                {
                    Key = _keyFactory(item),
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