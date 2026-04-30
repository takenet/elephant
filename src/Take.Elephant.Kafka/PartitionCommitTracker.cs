using System.Collections.Generic;

namespace Take.Elephant.Kafka
{
    /// <summary>
    /// Tracks acknowledged offsets for a single Kafka partition and returns the
    /// highest contiguous offset safe to commit (i.e., all offsets from the last
    /// committed point up to the returned offset have been acknowledged — no gaps).
    ///
    /// Each instance is scoped to one <see cref="Confluent.Kafka.TopicPartition"/>.
    /// A new instance must be created whenever the partition is (re-)assigned after
    /// a rebalance so that stale state from a previous assignment cannot block future
    /// commits. The rebalance handlers in <see cref="KafkaReceiverQueue{T}"/> replace
    /// the entry in <c>_partitionTrackers</c> with a fresh instance; the old instance
    /// remains alive as long as in-flight delegates from before the rebalance still
    /// hold a reference to it. Those delegates may call <see cref="Acknowledge"/> on
    /// the old instance, advance its HWM, and then attempt a Kafka commit — which will
    /// throw a <see cref="Confluent.Kafka.KafkaException"/> because the partition is no
    /// longer owned by this consumer. That exception is caught in the ack delegate and
    /// discarded; Kafka will redeliver the messages to the new owner.
    ///
    /// Algorithm:
    ///   - <see cref="Track"/> bootstraps the HWM on the first call and handles two
    ///     classes of intra-assignment offset discontinuity:
    ///     (a) Backward jumps (offset &lt; last tracked): caused by a manual
    ///         <c>consumer.Seek()</c> to a prior position. The HWM, acked set, and
    ///         phantom-gap queue are reset relative to the new position, and a new
    ///         epoch boundary is recorded so that in-flight delegates from before the
    ///         seek are discarded by <see cref="Acknowledge"/>.
    ///     (b) Forward gaps (offset &gt; last tracked + 1): caused by log compaction,
    ///         aborted transactions, or control records that are never delivered to
    ///         consumers. These are <b>not</b> treated as resets.
    ///         If no messages are currently in flight (all prior offsets have been
    ///         committed), the HWM is advanced in O(1) to skip the gap.
    ///         Otherwise, the gap is recorded as a <em>phantom range</em>
    ///         <c>(start, end)</c> in a FIFO queue. During HWM advancement the entire
    ///         range is skipped in a single O(1) dequeue, regardless of how large the
    ///         gap is. This avoids the O(gap) per-offset allocation that would be
    ///         required when pre-populating a sorted set with every phantom offset.
    ///   - <see cref="Acknowledge"/> adds the offset to the acked set and advances
    ///     the HWM while the next expected offset is either in the acked set or at the
    ///     start of a phantom range. Acks for offsets below the current epoch boundary
    ///     are discarded immediately to prevent stale delegates from causing a memory
    ///     leak or a spurious commit.
    ///
    /// Known limitation: after a backward seek to offset S, in-flight ack delegates
    /// created before the seek for offsets &gt;= S are not filtered (they share the
    /// same epoch). In the unlikely scenario where such a stale ack arrives before its
    /// corresponding replay-ack, it may accelerate the HWM. This is an inherent
    /// trade-off of single-epoch tracking and is acceptable given that manual seeks are
    /// rare in production consumers.
    ///
    /// Thread-safe.
    /// </summary>
    internal sealed class PartitionCommitTracker
    {
        private readonly SortedSet<long> _acked = new SortedSet<long>();
        // Phantom gap ranges: offsets that were never delivered (compacted / aborted records).
        // Stored in FIFO order (same order as Track() calls). Each entry is (start, end) inclusive
        // on both ends. The first entry in the queue is always the next phantom range the HWM will
        // encounter, which is why a Queue is sufficient — gaps arrive and are consumed in strict order.
        private readonly Queue<(long Start, long End)> _phantomGaps = new Queue<(long, long)>();
        private readonly object _lock = new object();
        private long _lastCommitted = long.MinValue; // uninitialized sentinel
        private long _lastTracked   = long.MinValue; // last offset seen in Track(); used for discontinuity detection
        private long _epochStart    = long.MinValue; // lowest valid offset of current epoch; stale acks below this are discarded

        /// <summary>
        /// Records that <paramref name="offset"/> was consumed. Must be called before
        /// the message is dispatched downstream. Bootstraps the HWM on the first call
        /// so that contiguity is relative to this run's starting offset.
        /// </summary>
        public void Track(long offset)
        {
            lock (_lock)
            {
                if (_lastTracked == long.MinValue)
                {
                    // First call: bootstrap HWM relative to this assignment's starting offset.
                    _lastCommitted = offset - 1;
                    _lastTracked   = offset;
                    _epochStart    = offset;
                    return;
                }

                if (offset != _lastTracked + 1)
                {
                    if (offset < _lastTracked)
                    {
                        // ── Backward jump: manual consumer.Seek() to a prior position ──────────
                        // The offsets between _lastTracked and offset will never arrive again.
                        // Reset HWM, the acked set, and the phantom-gap queue. Advance the epoch
                        // boundary so that any Acknowledge() calls still in flight for the old
                        // positions are silently discarded, preventing memory leaks and spurious
                        // commits.
                        _lastCommitted = offset - 1;
                        _acked.Clear();
                        _phantomGaps.Clear();
                        _epochStart = offset;
                    }
                    else if (_acked.Count == 0 && _lastCommitted == _lastTracked)
                    {
                        // ── Forward gap, no messages in flight ────────────────────────────────
                        // All previously tracked offsets have already been acked and committed
                        // (_lastCommitted == _lastTracked). The gap offsets (compacted, aborted
                        // transactions, control records) will never be delivered, so there is
                        // nothing to wait for. Advance _lastCommitted directly in O(1).
                        _lastCommitted = offset - 1;
                    }
                    else
                    {
                        // ── Forward gap with in-flight messages ───────────────────────────────
                        // Some earlier offsets are still being processed concurrently.
                        // Record the phantom range in O(1). The HWM advancement loop in
                        // Acknowledge() will skip the entire range with a single Dequeue()
                        // call when it reaches the start of the gap, avoiding any per-offset
                        // allocation regardless of how large the gap is.
                        _phantomGaps.Enqueue((_lastTracked + 1, offset - 1));
                    }
                }

                _lastTracked = offset;
            }
        }

        /// <summary>
        /// Marks <paramref name="offset"/> as acknowledged.
        /// Returns the offset value to commit (<c>hwm + 1</c>, per Kafka convention)
        /// when a new contiguous run from the last committed point is complete;
        /// otherwise returns <see langword="null"/>.
        /// </summary>
        public long? Acknowledge(long offset)
        {
            lock (_lock)
            {
                // Bootstrap defensively if Track was never called.
                if (_lastTracked == long.MinValue)
                {
                    _lastCommitted = offset - 1;
                    _lastTracked   = offset;
                    _epochStart    = offset;
                }

                // Discard stale acks from a previous epoch (e.g., delegates created before
                // a Seek that fire after the reset). These offsets will never advance the HWM
                // of the current epoch; keeping them in _acked would be a memory leak.
                if (offset < _epochStart)
                    return null;

                _acked.Add(offset);

                // Advance high-water mark while the next expected offset is either
                // in the acked set or at the head of the phantom-gap queue.
                var hwm = _lastCommitted;
                while (true)
                {
                    var next = hwm + 1;
                    if (_acked.Contains(next))
                    {
                        hwm++;
                        _acked.Remove(hwm);
                    }
                    else if (_phantomGaps.Count > 0 && _phantomGaps.Peek().Start == next)
                    {
                        // Skip the entire phantom range in one O(1) step.
                        hwm = _phantomGaps.Dequeue().End;
                    }
                    else
                    {
                        break;
                    }
                }

                if (hwm > _lastCommitted)
                {
                    _lastCommitted = hwm;
                    // Kafka convention: commit = last processed offset + 1
                    return hwm + 1;
                }

                return null;
            }
        }
    }
}

