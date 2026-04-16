using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Take.Elephant.Kafka
{
    /// <summary>
    /// Kafka receiver queue that supports three acknowledgement modes:
    /// <list type="bullet">
    ///   <item><see cref="KafkaAckMode.Eager"/> — legacy default: auto-commit managed by Confluent client.</item>
    ///   <item><see cref="KafkaAckMode.OnSuccess"/> — offset is stored only after <see cref="DequeueAckableAsync"/> returns and the caller invokes <see cref="KafkaAckableMessage{T}.AcknowledgeAsync"/>.</item>
    ///   <item><see cref="KafkaAckMode.Manual"/> — same as OnSuccess but the application controls when to ack independently of the pipeline.</item>
    /// </list>
    /// </summary>
    public class KafkaReceiverQueue<T> : IKafkaAckableReceiverQueue<T>, IOpenable, ICloseable, IDisposable
    {
        private IConsumer<Ignore, string> _consumer;
        private ISerializer<T> _serializer;
        private KafkaAckMode _ackMode;
        // Per-partition commit trackers — one instance per assigned partition (OnSuccess/Manual only)
        private ConcurrentDictionary<TopicPartition, PartitionCommitTracker> _partitionTrackers;
        private SemaphoreSlim _consumerStartSemaphore;
        private CancellationTokenSource _cts;
        // Eager channel: (Item, Headers)
        private Channel<(T Item, Headers KafkaHeaders)> _channel;
        // Ackable channel: includes partition/offset and the per-partition tracker snapshot
        private Channel<(T Item, Headers KafkaHeaders, TopicPartition Partition, long Offset, PartitionCommitTracker Tracker)> _ackableChannel;
        private Task _consumerTask;
        private bool _closed;

        public KafkaReceiverQueue(string bootstrapServers, string topic, string groupId, ISerializer<T> serializer, IDeserializer<string> deserializer = null)
            : this(new ConsumerConfig() { BootstrapServers = bootstrapServers, GroupId = groupId }, topic, serializer, deserializer)
        {
        }

        public KafkaReceiverQueue(
            ConsumerConfig consumerConfig,
            string topic,
            ISerializer<T> serializer,
            IDeserializer<string> deserializer = null,
            KafkaAckMode ackMode = KafkaAckMode.Eager)
        {
            ConcurrentDictionary<TopicPartition, PartitionCommitTracker> partitionTrackers = null;
            if (ackMode != KafkaAckMode.Eager)
            {
                // Disable auto-commit so we control offset stores
                consumerConfig = new ConsumerConfig(consumerConfig)
                {
                    EnableAutoCommit = false,
                    EnableAutoOffsetStore = false,
                };
                // Create the dictionary before wiring the handlers so the closures
                // capture the same reference that Initialize() will store.
                partitionTrackers = new ConcurrentDictionary<TopicPartition, PartitionCommitTracker>();
            }

            var builder = new ConsumerBuilder<Ignore, string>(consumerConfig)
                .SetValueDeserializer(deserializer ?? new StringDeserializer());

            if (partitionTrackers != null)
            {
                // On assignment: install a fresh tracker for each new partition so
                // state from any previous assignment cannot block future commits.
                // On revoke/lost: remove the tracker; the partition is no longer ours.
                builder = builder
                    .SetPartitionsAssignedHandler((c, partitions) =>
                    {
                        foreach (var tp in partitions)
                            partitionTrackers[tp] = new PartitionCommitTracker();
                    })
                    .SetPartitionsRevokedHandler((c, partitions) =>
                    {
                        foreach (var tpo in partitions)
                            partitionTrackers.TryRemove(tpo.TopicPartition, out _);
                    })
                    .SetPartitionsLostHandler((c, partitions) =>
                    {
                        foreach (var tpo in partitions)
                            partitionTrackers.TryRemove(tpo.TopicPartition, out _);
                    });
            }

            Initialize(builder.Build(), serializer, topic, ackMode, partitionTrackers);
        }

        /// <summary>
        /// Initializes a new instance using an already-built <see cref="IConsumer{TKey, TValue}"/>.
        /// Intended primarily for testing with mocked consumers.
        /// </summary>
        /// <remarks>
        /// When <paramref name="ackMode"/> is not <see cref="KafkaAckMode.Eager"/>, the caller is
        /// responsible for ensuring the consumer was built with
        /// <c>EnableAutoCommit = false</c> and <c>EnableAutoOffsetStore = false</c>.
        /// If the injected consumer has auto-commit or auto-offset-store enabled,
        /// offsets will be committed by the Confluent client regardless of
        /// <see cref="KafkaAckableMessage{T}.AcknowledgeAsync"/> calls,
        /// silently defeating the at-least-once guarantee.
        /// For production use, prefer the constructor that accepts <see cref="Confluent.Kafka.ConsumerConfig"/>.
        /// </remarks>
        public KafkaReceiverQueue(IConsumer<Ignore, string> consumer, ISerializer<T> serializer, string topic,
            KafkaAckMode ackMode = KafkaAckMode.Eager)
        {
            Initialize(consumer, serializer, topic, ackMode, null);
        }

        private void Initialize(IConsumer<Ignore, string> consumer, ISerializer<T> serializer, string topic,
            KafkaAckMode ackMode, ConcurrentDictionary<TopicPartition, PartitionCommitTracker> partitionTrackers)
        {
            if (string.IsNullOrWhiteSpace(topic)) throw new ArgumentException("Value cannot be null or whitespace.", nameof(topic));
            _consumer = consumer ?? throw new ArgumentNullException(nameof(consumer));
            ArgumentNullException.ThrowIfNull(serializer);
            _serializer = serializer;
            Topic = topic;
            _ackMode = ackMode;
            _consumerStartSemaphore = new SemaphoreSlim(1, 1);
            _cts = new CancellationTokenSource();

            if (ackMode == KafkaAckMode.Eager)
            {
                _channel = Channel.CreateBounded<(T, Headers)>(1);
            }
            else
            {
                // Use the pre-created dictionary (which already has rebalance handlers
                // wired via ConsumerBuilder) or create a fresh one for the IConsumer path.
                _partitionTrackers = partitionTrackers ?? new ConcurrentDictionary<TopicPartition, PartitionCommitTracker>();
                _ackableChannel = Channel.CreateBounded<(T, Headers, TopicPartition, long, PartitionCommitTracker)>(1);
            }
        }

        public string Topic { get; private set; }

        public virtual async Task<T> DequeueOrDefaultAsync(CancellationToken cancellationToken = default)
        {
            if (_ackMode != KafkaAckMode.Eager)
                throw new InvalidOperationException(
                    $"{nameof(DequeueOrDefaultAsync)} is not supported when {nameof(KafkaAckMode)} " +
                    $"is {_ackMode}. Use {nameof(DequeueAckableAsync)} or {nameof(DequeueAckableOrDefaultAsync)} " +
                    $"to obtain an acknowledgeable handle, otherwise offsets will never be committed.");

            await StartConsumerTaskIfNotAsync(cancellationToken);
            if (_channel.Reader.TryRead(out var item))
            {
                return item.Item;
            }

            return default;
        }

        public async Task<KafkaConsumedMessage<T>> DequeueWithHeadersOrDefaultAsync(CancellationToken cancellationToken = default)
        {
            if (_ackMode != KafkaAckMode.Eager)
                throw new InvalidOperationException(
                    $"{nameof(DequeueWithHeadersOrDefaultAsync)} is not supported when {nameof(KafkaAckMode)} " +
                    $"is {_ackMode}. Use {nameof(DequeueAckableAsync)} or {nameof(DequeueAckableOrDefaultAsync)} " +
                    $"to obtain an acknowledgeable handle, otherwise offsets will never be committed.");

            await StartConsumerTaskIfNotAsync(cancellationToken);
            if (_channel.Reader.TryRead(out var item))
            {
                return KafkaHeadersConverter.BuildConsumedMessage(item.Item, item.KafkaHeaders);
            }

            return default;
        }

        public virtual async Task<T> DequeueAsync(CancellationToken cancellationToken)
        {
            if (_ackMode != KafkaAckMode.Eager)
                throw new InvalidOperationException(
                    $"{nameof(DequeueAsync)} is not supported when {nameof(KafkaAckMode)} " +
                    $"is {_ackMode}. Use {nameof(DequeueAckableAsync)} to obtain an acknowledgeable " +
                    $"handle, otherwise offsets will never be committed.");

            await StartConsumerTaskIfNotAsync(cancellationToken);
            var item = await _channel.Reader.ReadAsync(cancellationToken);
            return item.Item;
        }

        public async Task<KafkaConsumedMessage<T>> DequeueWithHeadersAsync(CancellationToken cancellationToken)
        {
            if (_ackMode != KafkaAckMode.Eager)
                throw new InvalidOperationException(
                    $"{nameof(DequeueWithHeadersAsync)} is not supported when {nameof(KafkaAckMode)} " +
                    $"is {_ackMode}. Use {nameof(DequeueAckableAsync)} to obtain an acknowledgeable " +
                    $"handle, otherwise offsets will never be committed.");

            await StartConsumerTaskIfNotAsync(cancellationToken);
            var item = await _channel.Reader.ReadAsync(cancellationToken);
            return KafkaHeadersConverter.BuildConsumedMessage(item.Item, item.KafkaHeaders);
        }

        // ---- IKafkaAckableReceiverQueue<T> ----

        public async Task<KafkaAckableMessage<T>> DequeueAckableAsync(CancellationToken cancellationToken)
        {
            await StartConsumerTaskIfNotAsync(cancellationToken);
            if (_ackMode == KafkaAckMode.Eager)
                throw new InvalidOperationException(
                    $"DequeueAckableAsync requires KafkaAckMode.OnSuccess or Manual. Current mode: {_ackMode}.");

            var entry = await _ackableChannel.Reader.ReadAsync(cancellationToken);
            return BuildAckableMessage(entry);
        }

        public async Task<KafkaAckableMessage<T>> DequeueAckableOrDefaultAsync(CancellationToken cancellationToken = default)
        {
            await StartConsumerTaskIfNotAsync(cancellationToken);
            if (_ackMode == KafkaAckMode.Eager)
                throw new InvalidOperationException(
                    $"DequeueAckableOrDefaultAsync requires KafkaAckMode.OnSuccess or Manual. Current mode: {_ackMode}.");

            if (_ackableChannel.Reader.TryRead(out var entry))
                return BuildAckableMessage(entry);

            return null;
        }

        private KafkaAckableMessage<T> BuildAckableMessage(
            (T Item, Headers KafkaHeaders, TopicPartition Partition, long Offset, PartitionCommitTracker Tracker) entry)
        {
            var tp = entry.Partition;
            var offset = entry.Offset;
            // Capture the tracker instance at message-creation time (not the field).
            // If a rebalance happens before AcknowledgeAsync is called, the delegate
            // still references the OLD tracker for this partition; the new assignment
            // gets a brand-new tracker instance. Any commit attempted on the old
            // partition after revocation throws a KafkaException, which we catch below.
            var capturedTracker = entry.Tracker;
            var headers = KafkaHeadersConverter.ToReadOnlyDictionary(entry.KafkaHeaders);
            return new KafkaAckableMessage<T>(
                entry.Item,
                headers,
                tp.Topic,
                tp.Partition.Value,
                offset,
                _ =>
                {
                    var commitOffset = capturedTracker.Acknowledge(offset);
                    if (commitOffset.HasValue)
                    {
                        try
                        {
                            var tpo = new TopicPartitionOffset(tp, new Offset(commitOffset.Value));
                            _consumer.StoreOffset(tpo);
                            _consumer.Commit(new[] { tpo });
                        }
                        catch (KafkaException)
                        {
                            // The partition was revoked or rebalanced before the commit
                            // could be persisted. Kafka will redeliver these messages to
                            // the new owner — safe to discard this exception.
                        }
                    }
                    return Task.CompletedTask;
                });
        }

        public Task OpenAsync(CancellationToken cancellationToken)
        {
            return StartConsumerTaskIfNotAsync(cancellationToken);
        }

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

        public event EventHandler<ExceptionEventArgs> ConsumerFailed;

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing) return;
            if (!_closed)
            {
                _closed = true;
                _cts.Cancel();
                if (_consumerTask != null)
                {
                    _consumerTask.GetAwaiter().GetResult();
                }
                _consumer.Close();
            }

            _consumer.Dispose();
            _cts.Dispose();
            _consumerStartSemaphore.Dispose();
        }

        private async Task StartConsumerTaskIfNotAsync(CancellationToken cancellationToken)
        {
            if (_consumerTask != null) return;

            await _consumerStartSemaphore.WaitAsync(cancellationToken);
            try
            {
                _consumerTask ??= Task
                    .Factory
                    .StartNew(
                        () => ConsumeAsync(_cts.Token),
                        TaskCreationOptions.LongRunning)
                    .Unwrap();
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
                    var resultValue = _serializer.Deserialize(result.Message.Value);

                    if (_ackMode == KafkaAckMode.Eager)
                    {
                        await _channel.Writer.WriteAsync((resultValue, result.Message.Headers), cancellationToken);
                    }
                    else
                    {
                        var tp = result.TopicPartition;
                        var offset = result.Offset.Value;
                        // Obtain (or lazily create) the per-partition tracker, then
                        // record the offset before pushing the message to the channel
                        // so no ack can race ahead of its Track() call.
                        var tracker = _partitionTrackers.GetOrAdd(tp, _ => new PartitionCommitTracker());
                        tracker.Track(offset);
                        await _ackableChannel.Writer.WriteAsync(
                            (resultValue, result.Message.Headers, tp, offset, tracker),
                            cancellationToken);
                    }
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
                        Trace.TraceError("An unhandled exception occurred on KafkaReceiverQueue: {0}", ex);
                    }
                }
            }

            _consumer.Unsubscribe();
            if (_ackMode == KafkaAckMode.Eager)
                _channel.Writer.Complete();
            else
                _ackableChannel.Writer.Complete();
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }

    public class JsonDeserializer<T> : IDeserializer<T>
    {
        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            var json = Deserializers.Utf8.Deserialize(data, isNull, context);
            return JsonConvert.DeserializeObject<T>(json);
        }
    }

    public class StringDeserializer : IDeserializer<string>
    {
        public string Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            return Deserializers.Utf8.Deserialize(data, isNull, context);
        }
    }
}
