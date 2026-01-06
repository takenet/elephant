using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;

namespace Take.Elephant.Kafka.SchemaRegistry
{
    /// <summary>
    /// A Kafka event stream publisher that uses Schema Registry for serialization.
    /// </summary>
    /// <typeparam name="TKey">The type of the message key.</typeparam>
    /// <typeparam name="TEvent">The type of the event.</typeparam>
    public class KafkaSchemaRegistryEventStreamPublisher<TKey, TEvent> : IEventStreamPublisher<TKey, TEvent>, IDisposable
        where TEvent : class
    {
        private readonly IProducer<TKey, TEvent> _producer;
        private readonly ISchemaRegistryClient _schemaRegistryClient;
        private readonly bool _ownsSchemaRegistryClient;

        /// <summary>
        /// Creates a new instance of <see cref="KafkaSchemaRegistryEventStreamPublisher{TKey, TEvent}"/>.
        /// </summary>
        /// <param name="bootstrapServers">The Kafka bootstrap servers.</param>
        /// <param name="topic">The topic name.</param>
        /// <param name="schemaRegistryOptions">The Schema Registry options.</param>
        public KafkaSchemaRegistryEventStreamPublisher(
            string bootstrapServers,
            string topic,
            SchemaRegistryOptions schemaRegistryOptions)
            : this(new ProducerConfig { BootstrapServers = bootstrapServers }, topic, schemaRegistryOptions)
        {
        }

        /// <summary>
        /// Creates a new instance of <see cref="KafkaSchemaRegistryEventStreamPublisher{TKey, TEvent}"/>.
        /// </summary>
        /// <param name="producerConfig">The producer configuration.</param>
        /// <param name="topic">The topic name.</param>
        /// <param name="schemaRegistryOptions">The Schema Registry options.</param>
        public KafkaSchemaRegistryEventStreamPublisher(
            ProducerConfig producerConfig,
            string topic,
            SchemaRegistryOptions schemaRegistryOptions)
            : this(
                producerConfig,
                topic,
                new CachedSchemaRegistryClient(schemaRegistryOptions.SchemaRegistryConfig),
                schemaRegistryOptions,
                ownsSchemaRegistryClient: true)
        {
        }

        /// <summary>
        /// Creates a new instance of <see cref="KafkaSchemaRegistryEventStreamPublisher{TKey, TEvent}"/>.
        /// </summary>
        /// <param name="producerConfig">The producer configuration.</param>
        /// <param name="topic">The topic name.</param>
        /// <param name="schemaRegistryClient">The Schema Registry client.</param>
        /// <param name="schemaRegistryOptions">The Schema Registry options.</param>
        public KafkaSchemaRegistryEventStreamPublisher(
            ProducerConfig producerConfig,
            string topic,
            ISchemaRegistryClient schemaRegistryClient,
            SchemaRegistryOptions schemaRegistryOptions)
            : this(producerConfig, topic, schemaRegistryClient, schemaRegistryOptions, ownsSchemaRegistryClient: false)
        {
        }

        private KafkaSchemaRegistryEventStreamPublisher(
            ProducerConfig producerConfig,
            string topic,
            ISchemaRegistryClient schemaRegistryClient,
            SchemaRegistryOptions schemaRegistryOptions,
            bool ownsSchemaRegistryClient)
        {
            if (string.IsNullOrWhiteSpace(topic))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(topic));

            Topic = topic;
            _schemaRegistryClient = schemaRegistryClient ?? throw new ArgumentNullException(nameof(schemaRegistryClient));
            _ownsSchemaRegistryClient = ownsSchemaRegistryClient;

            var serializer = SchemaRegistrySerializerFactory.CreateSerializer<TEvent>(schemaRegistryClient, schemaRegistryOptions);
            _producer = new ProducerBuilder<TKey, TEvent>(producerConfig)
                .SetValueSerializer(serializer)
                .Build();
        }

        /// <summary>
        /// Creates a new instance of <see cref="KafkaSchemaRegistryEventStreamPublisher{TKey, TEvent}"/> using a pre-built producer.
        /// </summary>
        /// <param name="producer">The Kafka producer.</param>
        /// <param name="topic">The topic name.</param>
        /// <param name="schemaRegistryClient">The Schema Registry client (optional, for lifecycle management).</param>
        public KafkaSchemaRegistryEventStreamPublisher(
            IProducer<TKey, TEvent> producer,
            string topic,
            ISchemaRegistryClient schemaRegistryClient = null)
        {
            if (string.IsNullOrWhiteSpace(topic))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(topic));

            _producer = producer ?? throw new ArgumentNullException(nameof(producer));
            Topic = topic;
            _schemaRegistryClient = schemaRegistryClient;
            _ownsSchemaRegistryClient = false;
        }

        /// <summary>
        /// Gets the topic name.
        /// </summary>
        public string Topic { get; }

        /// <inheritdoc />
        public async Task PublishAsync(TKey key, TEvent item, CancellationToken cancellationToken)
        {
            await _producer.ProduceAsync(
                Topic,
                new Message<TKey, TEvent>
                {
                    Key = key,
                    Value = item
                },
                cancellationToken);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _producer?.Dispose();
                if (_ownsSchemaRegistryClient)
                {
                    _schemaRegistryClient?.Dispose();
                }
            }
        }
    }
}

