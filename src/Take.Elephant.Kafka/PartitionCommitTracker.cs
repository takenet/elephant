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
    ///   - <see cref="Track"/> bootstraps the high-water mark (HWM) on the first call
    ///     so contiguity is relative to this assignment's starting offset.
    ///   - <see cref="Acknowledge"/> adds the offset to the acked set and advances the
    ///     HWM while the next expected offset is present in the set.
    ///
    /// Thread-safe.
    /// </summary>
    /// <remarks>
    /// <b>Known limitation:</b> this class does not detect intra-assignment offset
    /// discontinuities (e.g., a manual <c>consumer.Seek()</c> within the same partition
    /// assignment). If Seek is called outside of a rebalance event, the tracker will
    /// wait indefinitely for missing offsets that will never arrive. In that scenario,
    /// discard this instance and create a new <see cref="PartitionCommitTracker"/>
    /// for the affected partition.
    /// </remarks>
    internal sealed class PartitionCommitTracker
    {
        private readonly SortedSet<long> _acked = new SortedSet<long>();
        private readonly object _lock = new object();
        private long _lastCommitted = long.MinValue; // uninitialized sentinel

        /// <summary>
        /// Records that <paramref name="offset"/> was consumed. Must be called before
        /// the message is dispatched downstream. Bootstraps the HWM on the first call
        /// so that contiguity is relative to this run's starting offset.
        /// </summary>
        public void Track(long offset)
        {
            lock (_lock)
            {
                if (_lastCommitted == long.MinValue)
                    _lastCommitted = offset - 1;
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
                // Bootstrap if Track was never called (purely defensive)
                if (_lastCommitted == long.MinValue)
                    _lastCommitted = offset - 1;

                _acked.Add(offset);

                // Advance high-water mark while the next expected offset is acked
                var hwm = _lastCommitted;
                while (_acked.Contains(hwm + 1))
                {
                    hwm++;
                    _acked.Remove(hwm);
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

