using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;

namespace Take.Elephant.Kafka.SchemaRegistry
{
    /// <summary>
    /// A Kafka partition queue (partition sender and receiver) that uses Schema Registry for serialization/deserialization.
    /// </summary>
    /// <typeparam name="T">The type of items in the queue.</typeparam>
    public class KafkaSchemaRegistryPartitionQueue<T> : IReceiverQueue<T>, IBlockingReceiverQueue<T>, IPartitionSenderQueue<T>, IOpenable, ICloseable, IDisposable where T : class
    {
        private readonly KafkaSchemaRegistryPartitionSenderQueue<T> _senderQueue;
        private readonly KafkaSchemaRegistryReceiverQueue<T> _receiverQueue;
        private readonly ISchemaRegistryClient _sharedSchemaRegistryClient;

        /// <summary>
        /// Creates a new instance of <see cref="KafkaSchemaRegistryPartitionQueue{T}"/>.
        /// </summary>
        /// <param name="bootstrapServers">The Kafka bootstrap servers.</param>
        /// <param name="topic">The topic name.</param>
        /// <param name="groupId">The consumer group ID.</param>
        /// <param name="schemaRegistryOptions">The Schema Registry options.</param>
        public KafkaSchemaRegistryPartitionQueue(
            string bootstrapServers,
            string topic,
            string groupId,
            SchemaRegistryOptions schemaRegistryOptions)
            : this(
                new ProducerConfig { BootstrapServers = bootstrapServers },
                new ConsumerConfig
                {
                    BootstrapServers = bootstrapServers,
                    GroupId = groupId,
                    AutoOffsetReset = AutoOffsetReset.Earliest
                },
                topic,
                schemaRegistryOptions)
        {
        }

        /// <summary>
        /// Creates a new instance of <see cref="KafkaSchemaRegistryPartitionQueue{T}"/>.
        /// </summary>
        /// <param name="producerConfig">The producer configuration.</param>
        /// <param name="consumerConfig">The consumer configuration.</param>
        /// <param name="topic">The topic name.</param>
        /// <param name="schemaRegistryOptions">The Schema Registry options.</param>
        public KafkaSchemaRegistryPartitionQueue(
            ProducerConfig producerConfig,
            ConsumerConfig consumerConfig,
            string topic,
            SchemaRegistryOptions schemaRegistryOptions)
        {
            // Create a shared Schema Registry client for both sender and receiver
            _sharedSchemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryOptions.SchemaRegistryConfig);

            _senderQueue = new KafkaSchemaRegistryPartitionSenderQueue<T>(
                producerConfig,
                topic,
                _sharedSchemaRegistryClient,
                schemaRegistryOptions);

            _receiverQueue = new KafkaSchemaRegistryReceiverQueue<T>(
                consumerConfig,
                topic,
                _sharedSchemaRegistryClient,
                schemaRegistryOptions);
        }

        /// <summary>
        /// Creates a new instance of <see cref="KafkaSchemaRegistryPartitionQueue{T}"/> with a shared Schema Registry client.
        /// </summary>
        /// <param name="producerConfig">The producer configuration.</param>
        /// <param name="consumerConfig">The consumer configuration.</param>
        /// <param name="topic">The topic name.</param>
        /// <param name="schemaRegistryClient">The Schema Registry client.</param>
        /// <param name="schemaRegistryOptions">The Schema Registry options.</param>
        public KafkaSchemaRegistryPartitionQueue(
            ProducerConfig producerConfig,
            ConsumerConfig consumerConfig,
            string topic,
            ISchemaRegistryClient schemaRegistryClient,
            SchemaRegistryOptions schemaRegistryOptions)
        {
            _senderQueue = new KafkaSchemaRegistryPartitionSenderQueue<T>(
                producerConfig,
                topic,
                schemaRegistryClient,
                schemaRegistryOptions);

            _receiverQueue = new KafkaSchemaRegistryReceiverQueue<T>(
                consumerConfig,
                topic,
                schemaRegistryClient,
                schemaRegistryOptions);
        }

        /// <inheritdoc />
        public Task EnqueueAsync(T item, string key, CancellationToken cancellationToken = default)
        {
            return _senderQueue.EnqueueAsync(item, key, cancellationToken);
        }

        /// <inheritdoc />
        public Task<T> DequeueOrDefaultAsync(CancellationToken cancellationToken = default)
        {
            return _receiverQueue.DequeueOrDefaultAsync(cancellationToken);
        }

        /// <inheritdoc />
        public Task<T> DequeueAsync(CancellationToken cancellationToken = default)
        {
            return _receiverQueue.DequeueAsync(cancellationToken);
        }

        /// <inheritdoc />
        public Task OpenAsync(CancellationToken cancellationToken)
        {
            return _receiverQueue.OpenAsync(cancellationToken);
        }

        /// <inheritdoc />
        public Task CloseAsync(CancellationToken cancellationToken) => _receiverQueue.CloseAsync(cancellationToken);

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
                _senderQueue?.Dispose();
                _receiverQueue?.Dispose();
                _sharedSchemaRegistryClient?.Dispose();
            }
        }
    }
}

