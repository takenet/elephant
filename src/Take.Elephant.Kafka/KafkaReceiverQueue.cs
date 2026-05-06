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
    /// Kafka receiver queue with three offset-commit modes: <see cref="KafkaAckMode.Eager"/> (auto-commit),
    /// <see cref="KafkaAckMode.OnSuccess"/>, and <see cref="KafkaAckMode.Manual"/>.
    /// Prefer the <see cref="KafkaReceiverQueue"/> static factory to guarantee the correct ack-mode
    /// configuration is applied; direct constructor calls may bypass required settings such as
    /// <c>EnableAutoCommit = false</c>.
    /// </summary>
    public class KafkaReceiverQueue<T> : IKafkaAckableReceiverQueue<T>, IKafkaReceiverQueue<T>, IOpenable, ICloseable, IDisposable
    {
        private IConsumer<Ignore, string> _consumer;
        private ISerializer<T> _serializer;
        private KafkaAckMode _ackMode;
        private ConcurrentDictionary<TopicPartition, PartitionCommitTracker> _partitionTrackers;
        // One Commit() in flight per partition — prevents cursor regression from out-of-order async acks.
        private ConcurrentDictionary<TopicPartition, SemaphoreSlim> _partitionCommitLocks;
        // Highest offset committed per partition; monotonic guard inside the per-partition lock.
        private ConcurrentDictionary<TopicPartition, long> _partitionMaxCommitted;
        private const long NoCommitsYet = long.MinValue;
        private SemaphoreSlim _consumerStartSemaphore;
        private CancellationTokenSource _cts;
        private Channel<(T Item, Headers KafkaHeaders)> _channel;
        private Channel<(T Item, Headers KafkaHeaders, TopicPartition Partition, long Offset, PartitionCommitTracker Tracker)> _ackableChannel;
        private Task _consumerTask;
        private volatile bool _closed;

        // Binary-compatible: preserves existing CLR signatures; defaults to Eager.
        public KafkaReceiverQueue(string bootstrapServers, string topic, string groupId, ISerializer<T> serializer, IDeserializer<string> deserializer = null)
            : this(bootstrapServers, topic, groupId, serializer, deserializer, KafkaAckMode.Eager)
        {
        }

        // IDeserializer<string> is required (not optional) to avoid CLR ambiguity with the 5-param overload above.
        // internal: callers must use KafkaReceiverQueue.Create* factory to get the correct narrowed interface.
        internal KafkaReceiverQueue(string bootstrapServers, string topic, string groupId, ISerializer<T> serializer, IDeserializer<string> deserializer, KafkaAckMode ackMode)
            : this(new ConsumerConfig() { BootstrapServers = bootstrapServers, GroupId = groupId }, topic, serializer, deserializer, ackMode)
        {
        }

        // Binary-compatible: preserves existing CLR signatures; defaults to Eager.
        public KafkaReceiverQueue(ConsumerConfig consumerConfig, string topic, ISerializer<T> serializer, IDeserializer<string> deserializer = null)
            : this(consumerConfig, topic, serializer, deserializer, KafkaAckMode.Eager)
        {
        }

        // IDeserializer<string> is required (not optional) to avoid CLR ambiguity with the 4-param overload above.
        // internal: callers must use KafkaReceiverQueue.Create* factory to get the correct narrowed interface.
        internal KafkaReceiverQueue(
            ConsumerConfig consumerConfig,
            string topic,
            ISerializer<T> serializer,
            IDeserializer<string> deserializer,
            KafkaAckMode ackMode)
        {
            ConcurrentDictionary<TopicPartition, PartitionCommitTracker> partitionTrackers = null;
            ConcurrentDictionary<TopicPartition, SemaphoreSlim> partitionCommitLocks = null;
            ConcurrentDictionary<TopicPartition, long> partitionMaxCommitted = null;
            if (ackMode != KafkaAckMode.Eager)
            {
                // Disable auto-commit; commits are driven by AcknowledgeAsync.
                consumerConfig = new ConsumerConfig(consumerConfig)
                {
                    EnableAutoCommit = false,
                    EnableAutoOffsetStore = false,
                };
                // Initialize per-partition state before wiring rebalance handlers.
                partitionTrackers = new ConcurrentDictionary<TopicPartition, PartitionCommitTracker>();
                partitionCommitLocks = new ConcurrentDictionary<TopicPartition, SemaphoreSlim>();
                partitionMaxCommitted = new ConcurrentDictionary<TopicPartition, long>();
            }

            var builder = new ConsumerBuilder<Ignore, string>(consumerConfig)
                .SetValueDeserializer(deserializer ?? new StringDeserializer());

            if (partitionTrackers != null)
            {
                builder = builder
                    .SetPartitionsAssignedHandler((c, partitions) =>
                    {
                        foreach (var tp in partitions)
                        {
                            partitionTrackers[tp] = new PartitionCommitTracker();
                            partitionCommitLocks[tp] = new SemaphoreSlim(1, 1);
                            partitionMaxCommitted[tp] = NoCommitsYet;
                        }
                    })
                    .SetPartitionsRevokedHandler((c, partitions) =>
                    {
                        foreach (var tpo in partitions)
                        {
                            var tp = tpo.TopicPartition;
                            partitionTrackers.TryRemove(tp, out _);
                            // SemaphoreSlim is NOT disposed here: in-flight ack delegates may still
                            // be waiting on it when revoke fires, and disposing with waiters throws
                            // ObjectDisposedException. The GC collects the instance once released.
                            partitionCommitLocks.TryRemove(tp, out _);
                            partitionMaxCommitted.TryRemove(tp, out _);
                        }
                    })
                    .SetPartitionsLostHandler((c, partitions) =>
                    {
                        foreach (var tpo in partitions)
                        {
                            var tp = tpo.TopicPartition;
                            partitionTrackers.TryRemove(tp, out _);
                            partitionCommitLocks.TryRemove(tp, out _);
                            partitionMaxCommitted.TryRemove(tp, out _);
                        }
                    });
            }

            Initialize(builder.Build(), serializer, topic, ackMode, partitionTrackers, partitionCommitLocks, partitionMaxCommitted);
        }

        // Binary-compatible: preserves existing CLR signatures.
        public KafkaReceiverQueue(IConsumer<Ignore, string> consumer, ISerializer<T> serializer, string topic)
            : this(consumer, serializer, topic, KafkaAckMode.Eager)
        {
        }

        /// <summary>
        /// Initializes with an already-built <see cref="IConsumer{TKey, TValue}"/>.
        /// Only <see cref="KafkaAckMode.Eager"/> is accepted; non-Eager requires
        /// <c>EnableAutoCommit = false</c> which cannot be guaranteed on an injected consumer.
        /// </summary>
        /// <exception cref="ArgumentException">Thrown when <paramref name="ackMode"/> is not Eager.</exception>
        public KafkaReceiverQueue(IConsumer<Ignore, string> consumer, ISerializer<T> serializer, string topic,
            KafkaAckMode ackMode = KafkaAckMode.Eager)
        {
            if (ackMode != KafkaAckMode.Eager)
                throw new ArgumentException(
                    $"Only {nameof(KafkaAckMode)}.{KafkaAckMode.Eager} is supported when injecting an " +
                    $"{nameof(IConsumer<Ignore, string>)} directly. For {ackMode} use the constructor " +
                    $"that accepts {nameof(ConsumerConfig)} so that EnableAutoCommit and " +
                    $"EnableAutoOffsetStore are enforced automatically.",
                    nameof(ackMode));

            Initialize(consumer, serializer, topic, ackMode, null, null, null);
        }

        // Test seam: allows injecting a mocked consumer with non-Eager mode.
        internal KafkaReceiverQueue(IConsumer<Ignore, string> consumer, ISerializer<T> serializer, string topic,
            KafkaAckMode ackMode, ConcurrentDictionary<TopicPartition, PartitionCommitTracker> partitionTrackers)
        {
            Initialize(consumer, serializer, topic, ackMode, partitionTrackers, null, null);
        }

        private void Initialize(
            IConsumer<Ignore, string> consumer,
            ISerializer<T> serializer,
            string topic,
            KafkaAckMode ackMode,
            ConcurrentDictionary<TopicPartition, PartitionCommitTracker> partitionTrackers,
            ConcurrentDictionary<TopicPartition, SemaphoreSlim> partitionCommitLocks,
            ConcurrentDictionary<TopicPartition, long> partitionMaxCommitted)
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
                _partitionTrackers = partitionTrackers ?? new ConcurrentDictionary<TopicPartition, PartitionCommitTracker>();
                _partitionCommitLocks = partitionCommitLocks ?? new ConcurrentDictionary<TopicPartition, SemaphoreSlim>();
                _partitionMaxCommitted = partitionMaxCommitted ?? new ConcurrentDictionary<TopicPartition, long>();
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

        public async Task<KafkaAckableMessage<T>> DequeueAckableAsync(CancellationToken cancellationToken)
        {
            if (_ackMode == KafkaAckMode.Eager)
                throw new InvalidOperationException(
                    $"DequeueAckableAsync requires KafkaAckMode.OnSuccess or Manual. Current mode: {_ackMode}.");

            await StartConsumerTaskIfNotAsync(cancellationToken);
            var entry = await _ackableChannel.Reader.ReadAsync(cancellationToken);
            return BuildAckableMessage(entry);
        }

        public async Task<KafkaAckableMessage<T>> DequeueAckableOrDefaultAsync(CancellationToken cancellationToken = default)
        {
            if (_ackMode == KafkaAckMode.Eager)
                throw new InvalidOperationException(
                    $"DequeueAckableOrDefaultAsync requires KafkaAckMode.OnSuccess or Manual. Current mode: {_ackMode}.");

            await StartConsumerTaskIfNotAsync(cancellationToken);

            if (_ackableChannel.Reader.TryRead(out var entry))
                return BuildAckableMessage(entry);

            return null;
        }

        private KafkaAckableMessage<T> BuildAckableMessage(
            (T Item, Headers KafkaHeaders, TopicPartition Partition, long Offset, PartitionCommitTracker Tracker) entry)
        {
            var tp = entry.Partition;
            var offset = entry.Offset;
            // Capture tracker at dequeue time; a post-dequeue rebalance gets a new tracker instance,
            // so stale ack delegates reference the old one and get rejected by the revocation check.
            var capturedTracker = entry.Tracker;
            var headers = KafkaHeadersConverter.ToReadOnlyDictionary(entry.KafkaHeaders);

            // Cached commit offset: reused on retry if a preceding Commit() threw,
            // since the tracker's HWM has already advanced and would return null on retry.
            long? cachedCommitOffset = null;

            return new KafkaAckableMessage<T>(
                entry.Item,
                headers,
                tp.Topic,
                tp.Partition.Value,
                offset,
                async ct =>
                {
                    // Use cached value from a failed retry, or compute fresh via tracker.
                    var commitOffset = cachedCommitOffset ?? capturedTracker.Acknowledge(offset);
                    if (!commitOffset.HasValue)
                        return;

                    cachedCommitOffset = commitOffset;

                    // Partition revoked since dequeue: new owner will redeliver.
                    if (!_partitionTrackers.TryGetValue(tp, out var currentTracker)
                        || !ReferenceEquals(currentTracker, capturedTracker))
                    {
                        cachedCommitOffset = null;
                        return;
                    }

                    // One Commit() per partition at a time: prevents a slow low-offset commit
                    // from regressing the broker cursor after a faster high-offset commit.
                    // GetOrAdd: safety-net for the test/IConsumer path where assignment handlers
                    // don't pre-populate the dict.
                    var commitLock = _partitionCommitLocks.GetOrAdd(tp, _ => new SemaphoreSlim(1, 1));
                    await commitLock.WaitAsync(ct);
                    try
                    {
                        // If a concurrent ack already committed a higher offset, skip.
                        long prevMax = _partitionMaxCommitted.GetOrAdd(tp, _ => NoCommitsYet);
                        if (commitOffset.Value <= prevMax)
                        {
                            cachedCommitOffset = null;
                            return;
                        }

                        // Update HWM before committing so concurrent waiters see it immediately.
                        _partitionMaxCommitted[tp] = commitOffset.Value;
                        try
                        {
                            // Bypass local offset store; push commit synchronously to the broker.
                            _consumer.Commit(new[] { new TopicPartitionOffset(tp, new Offset(commitOffset.Value)) });
                            cachedCommitOffset = null;
                        }
                        catch (KafkaException)
                            when (!_partitionTrackers.TryGetValue(tp, out var t2)
                                  || !ReferenceEquals(t2, capturedTracker))
                        {
                            // TOCTOU race: partition revoked between revocation-check and Commit.
                            // Kafka redelivers to new owner. TryRemove prevents stale prevMax from
                            // blocking the first commit of a rapid re-assignment.
                            _partitionMaxCommitted.TryRemove(tp, out _);
                            cachedCommitOffset = null;
                        }
                        catch (ObjectDisposedException)
                        {
                            // Consumer closed before ack fired; Kafka redelivers after restart.
                            _partitionMaxCommitted[tp] = prevMax;
                            cachedCommitOffset = null;
                        }
                        catch (InvalidOperationException) when (_closed)
                        {
                            // Consumer in invalid state during shutdown; same as ObjectDisposedException.
                            _partitionMaxCommitted[tp] = prevMax;
                            cachedCommitOffset = null;
                        }
                        // Other exceptions (broker unavailable, timeout, etc.) propagate to caller;
                        // prevMax is rolled back and cachedCommitOffset preserved so the caller can retry.
                        catch
                        {
                            _partitionMaxCommitted[tp] = prevMax;
                            throw;
                        }
                    }
                    finally
                    {
                        commitLock.Release();
                    }
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

                    if (_ackMode == KafkaAckMode.Eager)
                    {
                        var resultValue = _serializer.Deserialize(result.Message.Value);
                        await _channel.Writer.WriteAsync((resultValue, result.Message.Headers), cancellationToken);
                    }
                    else
                    {
                        var tp = result.TopicPartition;
                        var offset = result.Offset.Value;
                        var tracker = _partitionTrackers.GetOrAdd(tp, _ => new PartitionCommitTracker());
                        // Track before deserialize: if Deserialize throws for offset N, the tracker
                        // would never see N, allowing the next offset to bootstrap/advance past it
                        // and silently skip the poison record on commit.
                        tracker.Track(offset);
                        T resultValue;
                        try
                        {
                            resultValue = _serializer.Deserialize(result.Message.Value);
                        }
                        catch
                        {
                            // Auto-advance HWM past the poison record so the tracker never blocks
                            // future commits waiting for an offset that will never be acked.
                            // The exception still propagates to the ConsumerFailed handler.
                            tracker.Acknowledge(offset);
                            throw;
                        }
                        // Track before channel write: no ack can race ahead of its Track() call.
                        await _ackableChannel.Writer.WriteAsync(
                            (resultValue, result.Message.Headers, tp, offset, tracker),
                            cancellationToken);
                        // Clear the state-machine field so a revoked tracker becomes GC-eligible
                        // before the loop blocks again at _consumer.Consume().
                        tracker = null;
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

    /// <summary>
    /// Factory for <see cref="KafkaReceiverQueue{T}"/>. Returns the concrete type so callers
    /// retain access to lifecycle operations such as <see cref="KafkaReceiverQueue{T}.OpenAsync"/>,
    /// <see cref="KafkaReceiverQueue{T}.CloseAsync"/>, and <see cref="KafkaReceiverQueue{T}.Dispose"/>.
    /// <para>
    /// Note: <see cref="KafkaReceiverQueue{T}"/> implements both <see cref="IKafkaReceiverQueue{T}"/>
    /// and <see cref="IKafkaAckableReceiverQueue{T}"/>. Assigning a non-Eager instance to
    /// <see cref="IKafkaReceiverQueue{T}"/> and calling <c>Dequeue*</c> methods will throw
    /// <see cref="InvalidOperationException"/> at runtime. Always use the concrete type or
    /// <see cref="IKafkaAckableReceiverQueue{T}"/> for <see cref="KafkaAckMode.OnSuccess"/>
    /// and <see cref="KafkaAckMode.Manual"/> instances.
    /// </para>
    /// </summary>
    public static class KafkaReceiverQueue
    {
        public static KafkaReceiverQueue<T> CreateEager<T>(
            string bootstrapServers, string topic, string groupId,
            ISerializer<T> serializer, IDeserializer<string> deserializer = null)
            => new KafkaReceiverQueue<T>(bootstrapServers, topic, groupId, serializer, deserializer, KafkaAckMode.Eager);

        public static KafkaReceiverQueue<T> CreateEager<T>(
            ConsumerConfig config, string topic,
            ISerializer<T> serializer, IDeserializer<string> deserializer = null)
            => new KafkaReceiverQueue<T>(config, topic, serializer, deserializer, KafkaAckMode.Eager);

        public static KafkaReceiverQueue<T> CreateOnSuccess<T>(
            string bootstrapServers, string topic, string groupId,
            ISerializer<T> serializer, IDeserializer<string> deserializer = null)
            => new KafkaReceiverQueue<T>(bootstrapServers, topic, groupId, serializer, deserializer, KafkaAckMode.OnSuccess);

        public static KafkaReceiverQueue<T> CreateOnSuccess<T>(
            ConsumerConfig config, string topic,
            ISerializer<T> serializer, IDeserializer<string> deserializer = null)
            => new KafkaReceiverQueue<T>(config, topic, serializer, deserializer, KafkaAckMode.OnSuccess);

        public static KafkaReceiverQueue<T> CreateManual<T>(
            string bootstrapServers, string topic, string groupId,
            ISerializer<T> serializer, IDeserializer<string> deserializer = null)
            => new KafkaReceiverQueue<T>(bootstrapServers, topic, groupId, serializer, deserializer, KafkaAckMode.Manual);

        public static KafkaReceiverQueue<T> CreateManual<T>(
            ConsumerConfig config, string topic,
            ISerializer<T> serializer, IDeserializer<string> deserializer = null)
            => new KafkaReceiverQueue<T>(config, topic, serializer, deserializer, KafkaAckMode.Manual);
    }
}
