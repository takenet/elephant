using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Take.Elephant.Kafka
{
    public class KafkaEventStreamPublisher<TKey, TEvent> : IEventStreamPublisher<TKey, TEvent>, IDisposable
    {
        private readonly IProducer<TKey, TEvent> _producer;
        private readonly IKafkaHeaderProvider _headerProvider;

        public KafkaEventStreamPublisher(string bootstrapServers, string topic, ISerializer<TEvent> serializer)
            : this(new ProducerConfig() { BootstrapServers = bootstrapServers }, topic, serializer)
        {
        }

        public KafkaEventStreamPublisher(string bootstrapServers, string topic, Confluent.Kafka.ISerializer<TEvent> kafkaSerializer)
            : this(new ProducerConfig() { BootstrapServers = bootstrapServers }, topic, kafkaSerializer)
        {
        }

        public KafkaEventStreamPublisher(
            ProducerConfig producerConfig,
            string topic,
            ISerializer<TEvent> serializer)
            : this(
                  new ProducerBuilder<TKey, TEvent>(producerConfig)
                        .SetValueSerializer(new EventSerializer(serializer))
                        .Build(),
                  topic,
                  null)
        {
        }

        public KafkaEventStreamPublisher(
            ProducerConfig producerConfig,
            string topic,
            ISerializer<TEvent> serializer,
            IKafkaHeaderProvider headerProvider)
            : this(
                  new ProducerBuilder<TKey, TEvent>(producerConfig)
                        .SetValueSerializer(new EventSerializer(serializer))
                        .Build(),
                  topic,
                  headerProvider)
        {
        }

        public KafkaEventStreamPublisher(
            ProducerConfig producerConfig,
            string topic,
            Confluent.Kafka.ISerializer<TEvent> kafkaSerializer)
            : this(
                  new ProducerBuilder<TKey, TEvent>(producerConfig)
                        .SetValueSerializer(kafkaSerializer)
                        .Build(),
                  topic,
                  null)
        {
        }

        public KafkaEventStreamPublisher(
            ProducerConfig producerConfig,
            string topic,
            Confluent.Kafka.ISerializer<TEvent> kafkaSerializer,
            IKafkaHeaderProvider headerProvider)
            : this(
                  new ProducerBuilder<TKey, TEvent>(producerConfig)
                        .SetValueSerializer(kafkaSerializer)
                        .Build(),
                  topic,
                  headerProvider)
        {
        }

        public KafkaEventStreamPublisher(
            IProducer<TKey, TEvent> producer,
            string topic)
        {
            if (string.IsNullOrWhiteSpace(topic))
            {
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(topic));
            }

            if (producer == null)
             {
                 throw new ArgumentNullException(nameof(producer));
             }

            _producer = producer;
            Topic = topic;
            _headerProvider = null;
        }

        public KafkaEventStreamPublisher(
            IProducer<TKey, TEvent> producer,
            string topic,
            IKafkaHeaderProvider headerProvider)
        {
            if (string.IsNullOrWhiteSpace(topic))
            {
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(topic));
            }

            if (producer == null)
             {
                 throw new ArgumentNullException(nameof(producer));
             }

            _producer = producer;
            Topic = topic;
            _headerProvider = headerProvider;
        }

        public string Topic { get; }

        public async Task PublishAsync(TKey key, TEvent item, CancellationToken cancellationToken)
        {
            var message = new Message<TKey, TEvent>
            {
                Key = key,
                Value = item
            };

            if (_headerProvider != null)
            {
                message.Headers = [];
                var headers = _headerProvider.GetHeaders();
                 if (headers != null)
                {
                    foreach (var header in headers)
                     {
                         if (header == null || header.Key == null)
                         {
                             continue;
                         }
                         message.Headers.Add(header.Key, header.GetValueBytes());
                     }
                }
            }

            await _producer.ProduceAsync(Topic, message, cancellationToken);
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

        public class EventSerializer(ISerializer<TEvent> serializer) : Confluent.Kafka.ISerializer<TEvent>
        {
            public byte[] Serialize(TEvent data, SerializationContext context)
            {
                return Encoding.UTF8.GetBytes(serializer.Serialize(data));
            }
        }
    }
}
