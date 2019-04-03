using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Take.Elephant.Kafka
{
    public class KafkaSenderQueue<T> : ISenderQueue<T>, IDisposable
    {
        private readonly IProducer<string, T> _producer;
        private readonly string _topic;
        private readonly Func<T, string> _keyFactory;

        public KafkaSenderQueue(string bootstrapServers, string topic)
            : this(new ProducerConfig() { BootstrapServers = bootstrapServers }, topic)
        {
            
        }
        
        public KafkaSenderQueue(
            ProducerConfig producerConfig,
            string topic,
            Confluent.Kafka.ISerializer<T> serializer = null,
            Func<T, string> keyFactory = null)
        {
            if (string.IsNullOrWhiteSpace(topic))
            {
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(topic));
            }

            var builder = new ProducerBuilder<string, T>(producerConfig);
            if (serializer != null)
            {
                builder = builder.SetValueSerializer(serializer);
            }

            _producer = builder.Build();
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
}