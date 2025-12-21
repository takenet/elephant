using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;

namespace Take.Elephant.Kafka.SchemaRegistry
{
    /// <summary>
    /// A Kafka receiver queue that uses Schema Registry for deserialization.
    /// </summary>
    /// <typeparam name="T">The type of items in the queue.</typeparam>
    public class KafkaSchemaRegistryReceiverQueue<T> : IReceiverQueue<T>, IBlockingReceiverQueue<T>, IOpenable, ICloseable, IDisposable where T : class
    {
        private readonly IConsumer<Ignore, T> _consumer;
        private readonly ISchemaRegistryClient _schemaRegistryClient;
        private readonly bool _ownsSchemaRegistryClient;
        private readonly SemaphoreSlim _consumerStartSemaphore;
        private readonly CancellationTokenSource _cts;
        private readonly Channel<T> _channel;
        private Task _consumerTask;
        private bool _closed;

        /// <summary>
        /// Creates a new instance of <see cref="KafkaSchemaRegistryReceiverQueue{T}"/>.
        /// </summary>
        /// <param name="bootstrapServers">The Kafka bootstrap servers.</param>
        /// <param name="topic">The topic name.</param>
        /// <param name="groupId">The consumer group ID.</param>
        /// <param name="schemaRegistryOptions">The Schema Registry options.</param>
        public KafkaSchemaRegistryReceiverQueue(
            string bootstrapServers,
            string topic,
            string groupId,
            SchemaRegistryOptions schemaRegistryOptions)
            : this(
                new ConsumerConfig { BootstrapServers = bootstrapServers, GroupId = groupId },
                topic,
                schemaRegistryOptions)
        {
        }

        /// <summary>
        /// Creates a new instance of <see cref="KafkaSchemaRegistryReceiverQueue{T}"/>.
        /// </summary>
        /// <param name="consumerConfig">The consumer configuration.</param>
        /// <param name="topic">The topic name.</param>
        /// <param name="schemaRegistryOptions">The Schema Registry options.</param>
        public KafkaSchemaRegistryReceiverQueue(
            ConsumerConfig consumerConfig,
            string topic,
            SchemaRegistryOptions schemaRegistryOptions)
            : this(
                consumerConfig,
                topic,
                new CachedSchemaRegistryClient(schemaRegistryOptions.SchemaRegistryConfig),
                schemaRegistryOptions,
                ownsSchemaRegistryClient: true)
        {
        }

        /// <summary>
        /// Creates a new instance of <see cref="KafkaSchemaRegistryReceiverQueue{T}"/>.
        /// </summary>
        /// <param name="consumerConfig">The consumer configuration.</param>
        /// <param name="topic">The topic name.</param>
        /// <param name="schemaRegistryClient">The Schema Registry client.</param>
        /// <param name="schemaRegistryOptions">The Schema Registry options.</param>
        public KafkaSchemaRegistryReceiverQueue(
            ConsumerConfig consumerConfig,
            string topic,
            ISchemaRegistryClient schemaRegistryClient,
            SchemaRegistryOptions schemaRegistryOptions)
            : this(consumerConfig, topic, schemaRegistryClient, schemaRegistryOptions, ownsSchemaRegistryClient: false)
        {
        }

        private KafkaSchemaRegistryReceiverQueue(
            ConsumerConfig consumerConfig,
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

            var deserializer = SchemaRegistrySerializerFactory.CreateDeserializer<T>(schemaRegistryClient, schemaRegistryOptions);
            _consumer = new ConsumerBuilder<Ignore, T>(consumerConfig)
                .SetValueDeserializer(deserializer)
                .Build();

            _consumerStartSemaphore = new SemaphoreSlim(1, 1);
            _cts = new CancellationTokenSource();
            _channel = Channel.CreateBounded<T>(1);
        }

        /// <summary>
        /// Creates a new instance of <see cref="KafkaSchemaRegistryReceiverQueue{T}"/> using a pre-built consumer.
        /// </summary>
        /// <param name="consumer">The Kafka consumer.</param>
        /// <param name="topic">The topic name.</param>
        /// <param name="schemaRegistryClient">The Schema Registry client (optional, for lifecycle management).</param>
        public KafkaSchemaRegistryReceiverQueue(
            IConsumer<Ignore, T> consumer,
            string topic,
            ISchemaRegistryClient schemaRegistryClient = null)
        {
            if (string.IsNullOrWhiteSpace(topic))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(topic));

            _consumer = consumer ?? throw new ArgumentNullException(nameof(consumer));
            Topic = topic;
            _schemaRegistryClient = schemaRegistryClient;
            _ownsSchemaRegistryClient = false;
            _consumerStartSemaphore = new SemaphoreSlim(1, 1);
            _cts = new CancellationTokenSource();
            _channel = Channel.CreateBounded<T>(1);
        }

        /// <summary>
        /// Gets the topic name.
        /// </summary>
        public string Topic { get; }

        /// <summary>
        /// Occurs when the consumer fails.
        /// </summary>
        public event EventHandler<ExceptionEventArgs> ConsumerFailed;

        /// <inheritdoc />
        public virtual async Task<T> DequeueOrDefaultAsync(CancellationToken cancellationToken = default)
        {
            await StartConsumerTaskIfNotAsync(cancellationToken);
            if (_channel.Reader.TryRead(out var item))
            {
                return item;
            }

            return default;
        }

        /// <inheritdoc />
        public virtual async Task<T> DequeueAsync(CancellationToken cancellationToken)
        {
            await StartConsumerTaskIfNotAsync(cancellationToken);
            return await _channel.Reader.ReadAsync(cancellationToken);
        }

        /// <inheritdoc />
        public Task OpenAsync(CancellationToken cancellationToken)
        {
            return StartConsumerTaskIfNotAsync(cancellationToken);
        }

        /// <inheritdoc />
        public virtual async Task CloseAsync(CancellationToken cancellationToken)
        {
            if (!_closed)
            {
                _closed = true;
                await _cts.CancelAsync();
                if (_consumerTask != null)
                {
                    await _consumerTask;
                }
                _consumer.Close();
            }
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
                if (!_closed)
                {
                    _consumer.Close();
                }

                _consumer.Dispose();
                _cts.Dispose();
                _consumerStartSemaphore.Dispose();

                if (_ownsSchemaRegistryClient)
                {
                    _schemaRegistryClient?.Dispose();
                }
            }
        }

        private async Task StartConsumerTaskIfNotAsync(CancellationToken cancellationToken)
        {
            if (_consumerTask != null) return;

            await _consumerStartSemaphore.WaitAsync(cancellationToken);
            try
            {
                if (_consumerTask == null)
                {
                    _consumerTask = Task
                        .Factory
                        .StartNew(
                            () => ConsumeAsync(_cts.Token),
                            TaskCreationOptions.LongRunning)
                        .Unwrap();
                }
            }
            finally
            {
                _consumerStartSemaphore.Release();
            }
        }

        private async Task ConsumeAsync(CancellationToken cancellationToken)
        {
            if (_consumer.Subscription.All(s => s != Topic))
            {
                _consumer.Subscribe(Topic);
            }

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var result = _consumer.Consume(cancellationToken);
                    await _channel.Writer.WriteAsync(result.Message.Value, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    var handler = ConsumerFailed;
                    if (handler != null)
                    {
                        handler.Invoke(this, new ExceptionEventArgs(ex));
                    }
                    else
                    {
                        Trace.TraceError("An unhandled exception occurred on KafkaSchemaRegistryReceiverQueue: {0}", ex);
                    }
                }
            }

            _consumer.Unsubscribe();
            _channel.Writer.Complete();
        }
    }
}

