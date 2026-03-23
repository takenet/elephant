using System;
using System.Collections.Generic;
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
                CreateProducer(producerConfig, serializer),
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
                CreateProducer(producerConfig, serializer),
                  topic,
                  headerProvider)
        {
        }

        public KafkaEventStreamPublisher(
            ProducerConfig producerConfig,
            string topic,
            Confluent.Kafka.ISerializer<TEvent> kafkaSerializer)
            : this(
                CreateProducer(producerConfig, kafkaSerializer),
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
                CreateProducer(producerConfig, kafkaSerializer),
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
                var headers = _headerProvider.GetHeaders() ?? Array.Empty<IHeader>();
                foreach (var header in headers)
                {
                    if (header == null || header.Key == null)
                    {
                        continue;
                    }

                    message.Headers ??= new Headers();
                    message.Headers.Add(header.Key, header.GetValueBytes());
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

        private static IProducer<TKey, TEvent> CreateProducer(
            ProducerConfig producerConfig,
            ISerializer<TEvent> serializer)
        {
            ArgumentNullException.ThrowIfNull(producerConfig);
            ArgumentNullException.ThrowIfNull(serializer);

            return new ProducerBuilder<TKey, TEvent>(producerConfig)
                .SetValueSerializer(new EventSerializer(serializer))
                .Build();
        }

        private static IProducer<TKey, TEvent> CreateProducer(
            ProducerConfig producerConfig,
            Confluent.Kafka.ISerializer<TEvent> kafkaSerializer)
        {
            ArgumentNullException.ThrowIfNull(producerConfig);
            ArgumentNullException.ThrowIfNull(kafkaSerializer);

            return new ProducerBuilder<TKey, TEvent>(producerConfig)
                .SetValueSerializer(kafkaSerializer)
                .Build();
        }

        public class EventSerializer : Confluent.Kafka.ISerializer<TEvent>
        {
            private readonly ISerializer<TEvent> _serializer;

            public EventSerializer(ISerializer<TEvent> serializer)
            {
                ArgumentNullException.ThrowIfNull(serializer);
                _serializer = serializer;
            }

            public byte[] Serialize(TEvent data, SerializationContext context)
            {
                return Encoding.UTF8.GetBytes(_serializer.Serialize(data));
            }
        }
    }
}
