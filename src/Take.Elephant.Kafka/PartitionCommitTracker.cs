using System.Collections.Generic;

namespace Take.Elephant.Kafka
{
    /// <summary>
    /// Thread-safe HWM (high-water-mark) tracker for a single Kafka partition.
    /// Returns the next offset to commit only when all offsets from the last commit are contiguous.
    /// Handles forward gaps (compacted/aborted offsets) via O(1) phantom-range queuing and
    /// backward seeks via epoch-boundary reset. Scoped to one partition assignment; create a new
    /// instance on rebalance.
    /// </summary>
    internal sealed class PartitionCommitTracker
    {
        private readonly SortedSet<long> _acked = new SortedSet<long>();
        // FIFO ranges of offsets never delivered (compacted/aborted); consumed in strict order as HWM advances.
        private readonly Queue<(long Start, long End)> _phantomGaps = new Queue<(long, long)>();
        private readonly object _lock = new object();
        private long _lastCommitted = long.MinValue; // uninitialized sentinel
        private long _lastTracked   = long.MinValue; // last offset seen in Track()
        private long _epochStart    = long.MinValue; // lowest valid offset for current assignment
        // Monotonically increasing counter: incremented on every backward seek so that all
        // in-flight ack delegates from the old epoch are rejected, even for offsets that fall
        // within the new epoch's offset range.
        private long _currentEpoch  = 0;

        /// <summary>
        /// Records a consumed offset. Must be called before dispatching downstream.
        /// Returns the epoch id that must be passed to <see cref="Acknowledge"/> for this offset.
        /// </summary>
        public long Track(long offset)
        {
            lock (_lock)
            {
                if (_lastTracked == long.MinValue)
                {
                    // First call: bootstrap HWM relative to this assignment's starting offset.
                    _lastCommitted = offset - 1;
                    _lastTracked   = offset;
                    _epochStart    = offset;
                    return _currentEpoch;
                }

                if (offset != _lastTracked + 1)
                {
                    if (offset < _lastTracked)
                    {
                        // Backward seek: reset HWM, acked set, and phantom-gap queue.
                        // Advance epoch to invalidate ALL in-flight ack delegates from the old epoch,
                        // including those whose offset happens to fall within the new epoch's range.
                        _lastCommitted = offset - 1;
                        _acked.Clear();
                        _phantomGaps.Clear();
                        _epochStart = offset;
                        _currentEpoch++;
                    }
                    else if (_acked.Count == 0 && _lastCommitted == _lastTracked)
                    {
                        // Forward gap, no in-flight messages: advance HWM directly in O(1).
                        _lastCommitted = offset - 1;
                    }
                    else
                    {
                        // Forward gap with in-flight messages: record phantom range in O(1).
                        // Acknowledge() skips the entire range in a single Dequeue(), avoiding per-offset allocations.
                        _phantomGaps.Enqueue((_lastTracked + 1, offset - 1));
                    }
                }

                _lastTracked = offset;
                return _currentEpoch;
            }
        }

        /// <summary>
        /// Marks <paramref name="offset"/> as acknowledged for the given <paramref name="epochId"/>.
        /// Returns <c>hwm + 1</c> (Kafka commit convention) when a contiguous run is complete;
        /// otherwise <see langword="null"/>.
        /// Acks from a superseded epoch (e.g. issued before a backward seek) are silently discarded.
        /// </summary>
        public long? Acknowledge(long offset, long epochId)
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

                // Discard stale acks from a superseded epoch. This catches both offset-below-epochStart
                // stale acks AND in-flight acks for offsets that fall within the new epoch's range but
                // were tracked before the backward seek occurred.
                if (epochId != _currentEpoch)
                    return null;

                // Discard already-committed offsets to prevent unbounded _acked growth on duplicate acks.
                if (offset <= _lastCommitted)
                    return null;

                _acked.Add(offset);

                // Advance HWM while the next expected offset is acked or at the head of a phantom range.
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
                        hwm = _phantomGaps.Dequeue().End; // skip phantom range in O(1)
                    }
                    else
                    {
                        break;
                    }
                }

                if (hwm > _lastCommitted)
                {
                    _lastCommitted = hwm;
                    return hwm + 1; // Kafka convention: commit offset = last acked + 1
                }

                return null;
            }
        }
    }
}

