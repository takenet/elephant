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
        private const string TopicNullOrWhitespaceMessage = "Value cannot be null or whitespace.";
        private readonly IProducer<Null, T> _producer;
        private readonly ISchemaRegistryClient _schemaRegistryClient;
        private readonly bool _ownsSchemaRegistryClient;
        private readonly IKafkaHeaderProvider _headerProvider;

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
        /// <param name="bootstrapServers">The Kafka bootstrap servers.</param>
        /// <param name="topic">The topic name.</param>
        /// <param name="schemaRegistryOptions">The Schema Registry options.</param>
        /// <param name="headerProvider">
         /// An optional provider used to generate Kafka message headers for each item before it is sent.
         /// If <c>null</c>, no headers are added. Implementations may return <c>null</c> or an empty set
         /// to indicate that no headers should be attached for a given message.
         /// </param>
        public KafkaSchemaRegistrySenderQueue(
            string bootstrapServers,
            string topic,
            SchemaRegistryOptions schemaRegistryOptions,
            IKafkaHeaderProvider headerProvider)
            : this(new ProducerConfig { BootstrapServers = bootstrapServers }, topic, schemaRegistryOptions, headerProvider)
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
                new CachedSchemaRegistryClient((schemaRegistryOptions ?? throw new ArgumentNullException(nameof(schemaRegistryOptions))).SchemaRegistryConfig),  
                schemaRegistryOptions,
                ownsSchemaRegistryClient: true,
                headerProvider: null)
        {
        }

        /// <summary>
        /// Creates a new instance of <see cref="KafkaSchemaRegistrySenderQueue{T}"/>.
        /// </summary>
        /// <param name="producerConfig">The producer configuration.</param>
        /// <param name="topic">The topic name.</param>
        /// <param name="schemaRegistryOptions">The Schema Registry options.</param>
        /// <param name="headerProvider">
         /// An optional provider used to generate Kafka message headers for each item before it is sent.
         /// If <c>null</c>, no headers are added. Implementations may return <c>null</c> or an empty set
         /// to indicate that no headers should be attached for a given message.
         /// </param>
        public KafkaSchemaRegistrySenderQueue(
            ProducerConfig producerConfig,
            string topic,
            SchemaRegistryOptions schemaRegistryOptions,
            IKafkaHeaderProvider headerProvider)
            : this(
                producerConfig,
                topic,
                new CachedSchemaRegistryClient(schemaRegistryOptions.SchemaRegistryConfig),
                schemaRegistryOptions,
                ownsSchemaRegistryClient: true,
                headerProvider: headerProvider)
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
            : this(producerConfig, topic, schemaRegistryClient, schemaRegistryOptions, ownsSchemaRegistryClient: false, headerProvider: null)
        {
        }

        /// <summary>
        /// Creates a new instance of <see cref="KafkaSchemaRegistrySenderQueue{T}"/>.
        /// </summary>
        /// <param name="producerConfig">The producer configuration.</param>
        /// <param name="topic">The topic name.</param>
        /// <param name="schemaRegistryClient">The Schema Registry client.</param>
        /// <param name="schemaRegistryOptions">The Schema Registry options.</param>
        /// <param name="headerProvider">
         /// An optional provider used to generate Kafka message headers for each item before it is sent.
         /// If <c>null</c>, no headers are added. Implementations may return <c>null</c> or an empty set
         /// to indicate that no headers should be attached for a given message.
         /// </param>
        public KafkaSchemaRegistrySenderQueue(
            ProducerConfig producerConfig,
            string topic,
            ISchemaRegistryClient schemaRegistryClient,
            SchemaRegistryOptions schemaRegistryOptions,
            IKafkaHeaderProvider headerProvider)
            : this(producerConfig, topic, schemaRegistryClient, schemaRegistryOptions, ownsSchemaRegistryClient: false, headerProvider: headerProvider)
        {
        }

        private KafkaSchemaRegistrySenderQueue(
            ProducerConfig producerConfig,
            string topic,
            ISchemaRegistryClient schemaRegistryClient,
            SchemaRegistryOptions schemaRegistryOptions,
            bool ownsSchemaRegistryClient,
            IKafkaHeaderProvider headerProvider = null)
        {
            if (string.IsNullOrWhiteSpace(topic))
                throw new ArgumentException(TopicNullOrWhitespaceMessage, nameof(topic));

            Topic = topic;
            _schemaRegistryClient = schemaRegistryClient ?? throw new ArgumentNullException(nameof(schemaRegistryClient));
            _ownsSchemaRegistryClient = ownsSchemaRegistryClient;
            _headerProvider = headerProvider;

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
        public KafkaSchemaRegistrySenderQueue(
            IProducer<Null, T> producer,
            string topic)
        {
            if (string.IsNullOrWhiteSpace(topic))
                throw new ArgumentException(TopicNullOrWhitespaceMessage, nameof(topic));

            _producer = producer ?? throw new ArgumentNullException(nameof(producer));
            Topic = topic;
            _schemaRegistryClient = null;
            _ownsSchemaRegistryClient = false;
            _headerProvider = null;
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
            ISchemaRegistryClient schemaRegistryClient)
        {
            if (string.IsNullOrWhiteSpace(topic))
                throw new ArgumentException(TopicNullOrWhitespaceMessage, nameof(topic));

            _producer = producer ?? throw new ArgumentNullException(nameof(producer));
            Topic = topic;
            _schemaRegistryClient = schemaRegistryClient;
            _ownsSchemaRegistryClient = false;
            _headerProvider = null;
        }

        /// <summary>
        /// Creates a new instance of <see cref="KafkaSchemaRegistrySenderQueue{T}"/> using a pre-built producer.
        /// </summary>
        /// <param name="producer">The Kafka producer.</param>
        /// <param name="topic">The topic name.</param>
        /// <param name="schemaRegistryClient">The Schema Registry client (optional, for lifecycle management).</param>
        /// <param name="headerProvider">
         /// An optional provider used to generate Kafka message headers for each item before it is sent.
         /// If <c>null</c>, no headers are added. Implementations may return <c>null</c> or an empty set
         /// to indicate that no headers should be attached for a given message.
         /// </param>
        public KafkaSchemaRegistrySenderQueue(
            IProducer<Null, T> producer,
            string topic,
            ISchemaRegistryClient schemaRegistryClient,
            IKafkaHeaderProvider headerProvider)
        {
            if (string.IsNullOrWhiteSpace(topic))
                throw new ArgumentException(TopicNullOrWhitespaceMessage, nameof(topic));

            _producer = producer ?? throw new ArgumentNullException(nameof(producer));
            Topic = topic;
            _schemaRegistryClient = schemaRegistryClient;
            _ownsSchemaRegistryClient = false;
            _headerProvider = headerProvider;
        }

        /// <summary>
        /// Gets the topic name.
        /// </summary>
        public string Topic { get; }

        /// <inheritdoc />
        public virtual async Task EnqueueAsync(T item, CancellationToken cancellationToken = default)
        {
            var message = new Message<Null, T>
            {
                Key = null,
                Value = item
            };

            if (_headerProvider != null)
            {
                var headers = _headerProvider.GetHeaders();
                message.Headers = [];
                if (headers != null)
                {
                     foreach (var header in headers)
                     {
                         if (header == null || header.Key == null) continue;
                         message.Headers.Add(header.Key, header.GetValueBytes());
                     }
                }
            }

            await _producer.ProduceAsync(Topic, message, cancellationToken);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing) return;
            _producer?.Dispose();
            if (_ownsSchemaRegistryClient)
            {
                _schemaRegistryClient?.Dispose();
            }
        }
    }
}

