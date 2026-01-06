using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;

namespace Take.Elephant.Kafka.SchemaRegistry
{
    /// <summary>
    /// A Kafka sender queue that uses Schema Registry for serialization.
    /// </summary>
    /// <typeparam name="T">The type of items in the queue.</typeparam>
    public class KafkaSchemaRegistrySenderQueue<T> : ISenderQueue<T>, IDisposable where T : class
    {
        private readonly IProducer<Null, T> _producer;
        private readonly ISchemaRegistryClient _schemaRegistryClient;
        private readonly bool _ownsSchemaRegistryClient;

        /// <summary>
        /// Creates a new instance of <see cref="KafkaSchemaRegistrySenderQueue{T}"/>.
        /// </summary>
        /// <param name="bootstrapServers">The Kafka bootstrap servers.</param>
        /// <param name="topic">The topic name.</param>
        /// <param name="schemaRegistryOptions">The Schema Registry options.</param>
        public KafkaSchemaRegistrySenderQueue(
            string bootstrapServers,
            string topic,
            SchemaRegistryOptions schemaRegistryOptions)
            : this(new ProducerConfig { BootstrapServers = bootstrapServers }, topic, schemaRegistryOptions)
        {
        }

        /// <summary>
        /// Creates a new instance of <see cref="KafkaSchemaRegistrySenderQueue{T}"/>.
        /// </summary>
        /// <param name="producerConfig">The producer configuration.</param>
        /// <param name="topic">The topic name.</param>
        /// <param name="schemaRegistryOptions">The Schema Registry options.</param>
        public KafkaSchemaRegistrySenderQueue(
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
        /// Creates a new instance of <see cref="KafkaSchemaRegistrySenderQueue{T}"/>.
        /// </summary>
        /// <param name="producerConfig">The producer configuration.</param>
        /// <param name="topic">The topic name.</param>
        /// <param name="schemaRegistryClient">The Schema Registry client.</param>
        /// <param name="schemaRegistryOptions">The Schema Registry options.</param>
        public KafkaSchemaRegistrySenderQueue(
            ProducerConfig producerConfig,
            string topic,
            ISchemaRegistryClient schemaRegistryClient,
            SchemaRegistryOptions schemaRegistryOptions)
            : this(producerConfig, topic, schemaRegistryClient, schemaRegistryOptions, ownsSchemaRegistryClient: false)
        {
        }

        private KafkaSchemaRegistrySenderQueue(
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

            var serializer = SchemaRegistrySerializerFactory.CreateSerializer<T>(schemaRegistryClient, schemaRegistryOptions);
            _producer = new ProducerBuilder<Null, T>(producerConfig)
                .SetValueSerializer(serializer)
                .Build();
        }

        /// <summary>
        /// Creates a new instance of <see cref="KafkaSchemaRegistrySenderQueue{T}"/> using a pre-built producer.
        /// </summary>
        /// <param name="producer">The Kafka producer.</param>
        /// <param name="topic">The topic name.</param>
        /// <param name="schemaRegistryClient">The Schema Registry client (optional, for lifecycle management).</param>
        public KafkaSchemaRegistrySenderQueue(
            IProducer<Null, T> producer,
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
        public virtual async Task EnqueueAsync(T item, CancellationToken cancellationToken = default)
        {
            await _producer.ProduceAsync(
                Topic,
                new Message<Null, T>
                {
                    Key = null,
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

