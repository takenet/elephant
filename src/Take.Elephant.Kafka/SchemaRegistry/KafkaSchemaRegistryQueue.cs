using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;

namespace Take.Elephant.Kafka.SchemaRegistry
{
    /// <summary>
    /// A Kafka queue (sender and receiver) that uses Schema Registry for serialization/deserialization.
    /// </summary>
    /// <typeparam name="T">The type of items in the queue.</typeparam>
    public class KafkaSchemaRegistryQueue<T> : IBlockingQueue<T>, ICloseable, IDisposable where T : class
    {
        private readonly KafkaSchemaRegistrySenderQueue<T> _senderQueue;
        private readonly KafkaSchemaRegistryReceiverQueue<T> _receiverQueue;
        private readonly ISchemaRegistryClient _sharedSchemaRegistryClient;

        /// <summary>
        /// Creates a new instance of <see cref="KafkaSchemaRegistryQueue{T}"/>.
        /// </summary>
        /// <param name="bootstrapServers">The Kafka bootstrap servers.</param>
        /// <param name="topic">The topic name.</param>
        /// <param name="groupId">The consumer group ID.</param>
        /// <param name="schemaRegistryOptions">The Schema Registry options.</param>
        public KafkaSchemaRegistryQueue(
            string bootstrapServers,
            string topic,
            string groupId,
            SchemaRegistryOptions schemaRegistryOptions)
            : this(
                new ProducerConfig { BootstrapServers = bootstrapServers },
                new ConsumerConfig { BootstrapServers = bootstrapServers, GroupId = groupId },
                topic,
                schemaRegistryOptions)
        {
        }

        /// <summary>
        /// Creates a new instance of <see cref="KafkaSchemaRegistryQueue{T}"/>.
        /// </summary>
        /// <param name="producerConfig">The producer configuration.</param>
        /// <param name="consumerConfig">The consumer configuration.</param>
        /// <param name="topic">The topic name.</param>
        /// <param name="schemaRegistryOptions">The Schema Registry options.</param>
        public KafkaSchemaRegistryQueue(
            ProducerConfig producerConfig,
            ConsumerConfig consumerConfig,
            string topic,
            SchemaRegistryOptions schemaRegistryOptions)
        {
            // Create a shared Schema Registry client for both sender and receiver
            _sharedSchemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryOptions.SchemaRegistryConfig);

            _senderQueue = new KafkaSchemaRegistrySenderQueue<T>(
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
        /// Creates a new instance of <see cref="KafkaSchemaRegistryQueue{T}"/> with a shared Schema Registry client.
        /// </summary>
        /// <param name="producerConfig">The producer configuration.</param>
        /// <param name="consumerConfig">The consumer configuration.</param>
        /// <param name="topic">The topic name.</param>
        /// <param name="schemaRegistryClient">The Schema Registry client.</param>
        /// <param name="schemaRegistryOptions">The Schema Registry options.</param>
        public KafkaSchemaRegistryQueue(
            ProducerConfig producerConfig,
            ConsumerConfig consumerConfig,
            string topic,
            ISchemaRegistryClient schemaRegistryClient,
            SchemaRegistryOptions schemaRegistryOptions)
        {
            _senderQueue = new KafkaSchemaRegistrySenderQueue<T>(
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
        public virtual Task EnqueueAsync(T item, CancellationToken cancellationToken = default)
        {
            return _senderQueue.EnqueueAsync(item, cancellationToken);
        }

        /// <inheritdoc />
        public virtual Task<T> DequeueAsync(CancellationToken cancellationToken)
        {
            return _receiverQueue.DequeueAsync(cancellationToken);
        }

        /// <inheritdoc />
        public virtual Task<T> DequeueOrDefaultAsync(CancellationToken cancellationToken = default)
        {
            return _receiverQueue.DequeueOrDefaultAsync(cancellationToken);
        }

        /// <inheritdoc />
        public virtual Task CloseAsync(CancellationToken cancellationToken)
        {
            return _receiverQueue.CloseAsync(cancellationToken);
        }

        /// <inheritdoc />
        public virtual Task<long> GetLengthAsync(CancellationToken cancellationToken = default)
        {
            return Task.FromException<long>(
                new NotSupportedException(
                    "It is not possible to determine the number of unhandled messages on a Kafka topic"));
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
                _senderQueue?.Dispose();
                _receiverQueue?.Dispose();
                _sharedSchemaRegistryClient?.Dispose();
            }
        }
    }
}

