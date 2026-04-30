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
    ///   <item><see cref="KafkaAckMode.OnSuccess"/> — offset committed only after the caller invokes <see cref="KafkaAckableMessage{T}.AcknowledgeAsync"/>.</item>
    ///   <item><see cref="KafkaAckMode.Manual"/> — same as OnSuccess but acknowledgement timing is fully application-controlled.</item>
    /// </list>
    /// </summary>
    /// <remarks>
    /// This class implements both <see cref="IKafkaReceiverQueue{T}"/> and
    /// <see cref="IKafkaAckableReceiverQueue{T}"/>. The two interfaces are mode-exclusive;
    /// calling the wrong family of Dequeue methods throws <see cref="InvalidOperationException"/>.
    /// <para>
    /// <b>Preferred construction:</b> use the <see cref="KafkaReceiverQueue"/> static factory class
    /// instead of calling <c>new KafkaReceiverQueue&lt;T&gt;(...)</c> directly. The factory methods
    /// return the narrowest interface that matches the requested mode, turning accidental
    /// DI-container misregistration into a compile-time error rather than a runtime failure.
    /// </para>
    /// <code>
    /// // Eager (auto-commit) — returns IKafkaReceiverQueue&lt;T&gt;
    /// IKafkaReceiverQueue&lt;Order&gt; q = KafkaReceiverQueue.CreateEager&lt;Order&gt;(config, topic, s);
    ///
    /// // OnSuccess / Manual — returns IKafkaAckableReceiverQueue&lt;T&gt;
    /// IKafkaAckableReceiverQueue&lt;Order&gt; q = KafkaReceiverQueue.CreateOnSuccess&lt;Order&gt;(config, topic, s);
    ///
    /// // The following is a COMPILE ERROR — compile-time protection against misregistration:
    /// // IKafkaReceiverQueue&lt;Order&gt; q = KafkaReceiverQueue.CreateOnSuccess&lt;Order&gt;(…);
    /// </code>
    /// </remarks>
    public class KafkaReceiverQueue<T> : IKafkaAckableReceiverQueue<T>, IKafkaReceiverQueue<T>, IOpenable, ICloseable, IDisposable
    {
        private IConsumer<Ignore, string> _consumer;
        private ISerializer<T> _serializer;
        private KafkaAckMode _ackMode;
        // Per-partition commit trackers — one instance per assigned partition (OnSuccess/Manual only)
        private ConcurrentDictionary<TopicPartition, PartitionCommitTracker> _partitionTrackers;
        // Per-partition commit serialization (OnSuccess/Manual only).
        // Ensures only one Commit() call is in flight per partition at a time, preventing
        // a slower lower-offset commit from moving the broker cursor backwards when
        // multiple messages are acknowledged concurrently.
        private ConcurrentDictionary<TopicPartition, SemaphoreSlim> _partitionCommitLocks;
        // Per-partition highest offset value we have committed so far (OnSuccess/Manual only).
        // Used inside the per-partition commit lock to skip commits that are no longer the
        // highest — guards against a slow Commit(N) arriving after a fast Commit(M > N) and
        // regressing the broker cursor.
        private ConcurrentDictionary<TopicPartition, long> _partitionMaxCommitted;
        // Sentinel that represents "no broker commit has been issued for this partition yet."
        private const long NoCommitsYet = long.MinValue;
        private SemaphoreSlim _consumerStartSemaphore;
        private CancellationTokenSource _cts;
        // Eager channel: (Item, Headers)
        private Channel<(T Item, Headers KafkaHeaders)> _channel;
        // Ackable channel: includes partition/offset and the per-partition tracker snapshot
        private Channel<(T Item, Headers KafkaHeaders, TopicPartition Partition, long Offset, PartitionCommitTracker Tracker)> _ackableChannel;
        private Task _consumerTask;
        private volatile bool _closed;

        // ── Binary-compatible overload: preserves the original 5-param CLR signature.
        // Assemblies compiled against the previous NuGet version of this library call
        // the constructor without ackMode. This explicit overload keeps those binaries
        // working after the package upgrade (defaults to Eager auto-commit).
        public KafkaReceiverQueue(string bootstrapServers, string topic, string groupId, ISerializer<T> serializer, IDeserializer<string> deserializer = null)
            : this(bootstrapServers, topic, groupId, serializer, deserializer, KafkaAckMode.Eager)
        {
        }

        // New overload that accepts an explicit ackMode.
        // IDeserializer<string> is required (not optional) to avoid CLR ambiguity with the
        // binary-compatible 5-param overload above.
        public KafkaReceiverQueue(string bootstrapServers, string topic, string groupId, ISerializer<T> serializer, IDeserializer<string> deserializer, KafkaAckMode ackMode)
            : this(new ConsumerConfig() { BootstrapServers = bootstrapServers, GroupId = groupId }, topic, serializer, deserializer, ackMode)
        {
        }

        // ── Binary-compatible overload: preserves the original 4-param CLR signature.
        public KafkaReceiverQueue(ConsumerConfig consumerConfig, string topic, ISerializer<T> serializer, IDeserializer<string> deserializer = null)
            : this(consumerConfig, topic, serializer, deserializer, KafkaAckMode.Eager)
        {
        }

        // New overload that accepts an explicit ackMode.
        // IDeserializer<string> is required (not optional) to avoid CLR ambiguity with the
        // binary-compatible 4-param overload above.
        public KafkaReceiverQueue(
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
                // Disable auto-commit so we control offset stores
                consumerConfig = new ConsumerConfig(consumerConfig)
                {
                    EnableAutoCommit = false,
                    EnableAutoOffsetStore = false,
                };
                // Create the dictionaries before wiring the handlers so the closures
                // capture the same references that Initialize() will store.
                partitionTrackers = new ConcurrentDictionary<TopicPartition, PartitionCommitTracker>();
                partitionCommitLocks = new ConcurrentDictionary<TopicPartition, SemaphoreSlim>();
                partitionMaxCommitted = new ConcurrentDictionary<TopicPartition, long>();
            }

            var builder = new ConsumerBuilder<Ignore, string>(consumerConfig)
                .SetValueDeserializer(deserializer ?? new StringDeserializer());

            if (partitionTrackers != null)
            {
                // On assignment: install a fresh tracker + commit state for each new partition
                // so that state from any previous assignment cannot block future commits.
                // On revoke/lost: remove all per-partition state; the partition is no longer
                // ours, so Kafka will redeliver to the new owner.
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
                            // The SemaphoreSlim is intentionally NOT disposed here: in-flight
                            // ack delegates may still be waiting on it when revoke fires, and
                            // disposing a SemaphoreSlim with waiters throws ObjectDisposedException.
                            // The GC collects the instance once all waiters release.
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
                            partitionCommitLocks.TryRemove(tp, out _); // same note as above
                            partitionMaxCommitted.TryRemove(tp, out _);
                        }
                    });
            }

            Initialize(builder.Build(), serializer, topic, ackMode, partitionTrackers, partitionCommitLocks, partitionMaxCommitted);
        }

        /// <summary>
        /// Initializes a new instance using an already-built <see cref="IConsumer{TKey, TValue}"/>.
        /// Only <see cref="KafkaAckMode.Eager"/> is accepted; passing any other mode throws
        /// <see cref="ArgumentException"/> because a consumer injected from outside cannot be
        /// guaranteed to have <c>EnableAutoCommit = false</c> and <c>EnableAutoOffsetStore = false</c>,
        /// which are required for at-least-once delivery.
        /// For <see cref="KafkaAckMode.OnSuccess"/> or <see cref="KafkaAckMode.Manual"/>, use the
        /// constructor that accepts <see cref="Confluent.Kafka.ConsumerConfig"/>.
        /// </summary>
        /// <remarks>
        /// Binary-compatible overload. Calls <see cref="KafkaReceiverQueue{T}(IConsumer{Ignore,string},ISerializer{T},string,KafkaAckMode)"/>
        /// with <see cref="KafkaAckMode.Eager"/>.
        /// </remarks>
        public KafkaReceiverQueue(IConsumer<Ignore, string> consumer, ISerializer<T> serializer, string topic)
            : this(consumer, serializer, topic, KafkaAckMode.Eager)
        {
        }

        /// <summary>
        /// Initializes a new instance using an already-built <see cref="IConsumer{TKey, TValue}"/>.
        /// Only <see cref="KafkaAckMode.Eager"/> is accepted; passing any other mode throws
        /// <see cref="ArgumentException"/> because a consumer injected from outside cannot be
        /// guaranteed to have <c>EnableAutoCommit = false</c> and <c>EnableAutoOffsetStore = false</c>,
        /// which are required for at-least-once delivery.
        /// For <see cref="KafkaAckMode.OnSuccess"/> or <see cref="KafkaAckMode.Manual"/>, use the
        /// constructor that accepts <see cref="Confluent.Kafka.ConsumerConfig"/>.
        /// </summary>
        /// <exception cref="ArgumentException">
        /// Thrown when <paramref name="ackMode"/> is not <see cref="KafkaAckMode.Eager"/>.
        /// </exception>
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

        /// <summary>
        /// Internal constructor used exclusively by unit tests that need to inject a mocked consumer
        /// with a non-Eager <see cref="KafkaAckMode"/>.
        /// The caller must ensure the mock is configured with <c>EnableAutoCommit = false</c> and
        /// <c>EnableAutoOffsetStore = false</c> to preserve the at-least-once guarantee.
        /// </summary>
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
                // Use the pre-created dictionaries (which already have rebalance handlers
                // wired via ConsumerBuilder) or create fresh ones for the IConsumer path.
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

        // ---- IKafkaAckableReceiverQueue<T> ----

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
            // Capture the tracker instance at message-creation time (not the field).
            // If a rebalance happens before AcknowledgeAsync is called, the delegate
            // still references the OLD tracker for this partition; the new assignment
            // gets a brand-new tracker instance. Any commit attempted on the old
            // partition after revocation throws a KafkaException, which we catch below.
            var capturedTracker = entry.Tracker;
            var headers = KafkaHeadersConverter.ToReadOnlyDictionary(entry.KafkaHeaders);

            // Cached commit offset: set the first time Acknowledge() computes the HWM;
            // reused on retry if the preceding Commit() call threw an exception.
            // The tracker's HWM has already advanced at that point, so re-calling
            // capturedTracker.Acknowledge(offset) would return null on a retry.
            long? cachedCommitOffset = null;

            return new KafkaAckableMessage<T>(
                entry.Item,
                headers,
                tp.Topic,
                tp.Partition.Value,
                offset,
                async ct =>
                {
                    // Use cached value from a previous failed attempt, or compute fresh via tracker.
                    var commitOffset = cachedCommitOffset ?? capturedTracker.Acknowledge(offset);
                    if (!commitOffset.HasValue)
                        return;

                    // Preserve for any retry — cleared only after a successful commit or
                    // when a commit is intentionally skipped (revocation/shutdown).
                    cachedCommitOffset = commitOffset;

                    // Early-exit: if this partition has been revoked since the message was
                    // dequeued, the rebalance handler will have removed or replaced the tracker
                    // in _partitionTrackers. Attempting a Commit would throw; skip it — Kafka
                    // will redeliver the messages to the new owner.
                    if (!_partitionTrackers.TryGetValue(tp, out var currentTracker)
                        || !ReferenceEquals(currentTracker, capturedTracker))
                    {
                        cachedCommitOffset = null; // partition is gone; no future retry makes sense
                        return;
                    }

                    // Serialize commits per partition: only one Commit() in flight at a time.
                    // This prevents a slower low-offset commit from arriving at the broker AFTER
                    // a faster high-offset commit and regressing the cursor backwards.
                    // GetOrAdd is a safety-net for the test/IConsumer path where rebalance handlers
                    // don't pre-populate the dict; in production the assignment handler always
                    // initializes the lock before the first message is produced to the channel.
                    var commitLock = _partitionCommitLocks.GetOrAdd(tp, _ => new SemaphoreSlim(1, 1));
                    await commitLock.WaitAsync(ct);
                    try
                    {
                        // Re-verify after acquiring the lock: if another concurrent ack already
                        // committed a higher offset while we were waiting, our commit is redundant.
                        long prevMax = _partitionMaxCommitted.GetOrAdd(tp, _ => NoCommitsYet);
                        if (commitOffset.Value <= prevMax)
                        {
                            cachedCommitOffset = null; // superseded by a concurrent higher ack
                            return;
                        }

                        // Advance the high-water mark before issuing the commit so that
                        // concurrent waiters see the updated value immediately.
                        _partitionMaxCommitted[tp] = commitOffset.Value;
                        try
                        {
                            // Commit explicit offsets directly to the broker.
                            // StoreOffset is intentionally omitted: Commit(IEnumerable<TopicPartitionOffset>)
                            // bypasses the local offset store and pushes the commit synchronously,
                            // so a preceding StoreOffset call would be redundant.
                            _consumer.Commit(new[] { new TopicPartitionOffset(tp, new Offset(commitOffset.Value)) });
                            cachedCommitOffset = null; // committed successfully; clear cache
                        }
                        catch (KafkaException)
                            when (!_partitionTrackers.TryGetValue(tp, out var t2)
                                  || !ReferenceEquals(t2, capturedTracker))
                        {
                            // TOCTOU race: partition was revoked between the check above and the
                            // Commit call. Kafka will redeliver to the new owner — safe to discard.
                            // Do NOT write prevMax back: the assignment handler for a rapid
                            // re-assignment of the same partition may have already reset the entry
                            // to NoCommitsYet. Overwriting it with prevMax would block the first
                            // commit of the new assignment. TryRemove is safe and idempotent —
                            // the next GetOrAdd will recreate the entry with NoCommitsYet.
                            _partitionMaxCommitted.TryRemove(tp, out _);
                            cachedCommitOffset = null;
                        }
                        catch (ObjectDisposedException)
                        {
                            // The consumer was closed (CloseAsync/Dispose) before this
                            // ack delegate fired. Treat as a graceful shutdown — Kafka
                            // will redeliver uncommitted offsets after restart.
                            _partitionMaxCommitted[tp] = prevMax; // roll back to ensure uncommitted offset is reprocessed after restart
                            cachedCommitOffset = null;
                        }
                        catch (InvalidOperationException) when (_closed)
                        {
                            // Consumer entered an invalid state during shutdown.
                            // Same treatment as ObjectDisposedException above.
                            _partitionMaxCommitted[tp] = prevMax; // roll back to ensure uncommitted offset is reprocessed after restart
                            cachedCommitOffset = null;
                        }
                        // All other exceptions (broker unavailable, authorization failure,
                        // timeout, etc.) propagate to the caller; _partitionMaxCommitted is
                        // rolled back and cachedCommitOffset remains set so the caller can
                        // retry by calling AcknowledgeAsync() again.
                        catch
                        {
                            _partitionMaxCommitted[tp] = prevMax; // roll back to allow retry with correct offset
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

    /// <summary>
    /// Type-safe factory for creating <see cref="KafkaReceiverQueue{T}"/> instances.
    /// Returns the narrowest interface matching the requested <see cref="KafkaAckMode"/>,
    /// turning accidental DI-container misregistration of a non-Eager queue under the legacy
    /// <see cref="IKafkaReceiverQueue{T}"/> contract into a <b>compile-time error</b>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Because <see cref="IKafkaAckableReceiverQueue{T}"/> is intentionally <b>not</b> derived
    /// from <see cref="IKafkaReceiverQueue{T}"/>, the following is a compile error:
    /// </para>
    /// <code>
    /// // CS0266 — cannot implicitly convert IKafkaAckableReceiverQueue&lt;T&gt; to IKafkaReceiverQueue&lt;T&gt;
    /// IKafkaReceiverQueue&lt;Order&gt; q = KafkaReceiverQueue.CreateOnSuccess&lt;Order&gt;(…);
    /// </code>
    /// <para>
    /// Compare with <c>new KafkaReceiverQueue&lt;Order&gt;(…, KafkaAckMode.OnSuccess)</c>, which
    /// compiles fine but throws <see cref="InvalidOperationException"/> at runtime when the
    /// wrong Dequeue* family is called.
    /// </para>
    /// </remarks>
    public static class KafkaReceiverQueue
    {
        // ── Eager (auto-commit) ──────────────────────────────────────────────

        /// <summary>
        /// Creates a queue in <see cref="KafkaAckMode.Eager"/> mode (auto-commit).
        /// </summary>
        /// <returns><see cref="IKafkaReceiverQueue{T}"/> — the non-ackable interface.</returns>
        public static IKafkaReceiverQueue<T> CreateEager<T>(
            string bootstrapServers, string topic, string groupId,
            ISerializer<T> serializer, IDeserializer<string> deserializer = null)
            => new KafkaReceiverQueue<T>(bootstrapServers, topic, groupId, serializer, deserializer, KafkaAckMode.Eager);

        /// <inheritdoc cref="CreateEager{T}(string,string,string,ISerializer{T},IDeserializer{string})"/>
        public static IKafkaReceiverQueue<T> CreateEager<T>(
            ConsumerConfig config, string topic,
            ISerializer<T> serializer, IDeserializer<string> deserializer = null)
            => new KafkaReceiverQueue<T>(config, topic, serializer, deserializer, KafkaAckMode.Eager);

        // ── OnSuccess ────────────────────────────────────────────────────────

        /// <summary>
        /// Creates a queue in <see cref="KafkaAckMode.OnSuccess"/> mode.
        /// The offset is committed only after the caller invokes
        /// <see cref="KafkaAckableMessage{T}.AcknowledgeAsync"/>.
        /// </summary>
        /// <returns><see cref="IKafkaAckableReceiverQueue{T}"/> — the ackable interface.</returns>
        public static IKafkaAckableReceiverQueue<T> CreateOnSuccess<T>(
            string bootstrapServers, string topic, string groupId,
            ISerializer<T> serializer, IDeserializer<string> deserializer = null)
            => new KafkaReceiverQueue<T>(bootstrapServers, topic, groupId, serializer, deserializer, KafkaAckMode.OnSuccess);

        /// <inheritdoc cref="CreateOnSuccess{T}(string,string,string,ISerializer{T},IDeserializer{string})"/>
        public static IKafkaAckableReceiverQueue<T> CreateOnSuccess<T>(
            ConsumerConfig config, string topic,
            ISerializer<T> serializer, IDeserializer<string> deserializer = null)
            => new KafkaReceiverQueue<T>(config, topic, serializer, deserializer, KafkaAckMode.OnSuccess);

        // ── Manual ───────────────────────────────────────────────────────────

        /// <summary>
        /// Creates a queue in <see cref="KafkaAckMode.Manual"/> mode.
        /// Acknowledgement timing is fully application-controlled.
        /// </summary>
        /// <returns><see cref="IKafkaAckableReceiverQueue{T}"/> — the ackable interface.</returns>
        public static IKafkaAckableReceiverQueue<T> CreateManual<T>(
            string bootstrapServers, string topic, string groupId,
            ISerializer<T> serializer, IDeserializer<string> deserializer = null)
            => new KafkaReceiverQueue<T>(bootstrapServers, topic, groupId, serializer, deserializer, KafkaAckMode.Manual);

        /// <inheritdoc cref="CreateManual{T}(string,string,string,ISerializer{T},IDeserializer{string})"/>
        public static IKafkaAckableReceiverQueue<T> CreateManual<T>(
            ConsumerConfig config, string topic,
            ISerializer<T> serializer, IDeserializer<string> deserializer = null)
            => new KafkaReceiverQueue<T>(config, topic, serializer, deserializer, KafkaAckMode.Manual);
    }
}
