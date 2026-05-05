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
        private long _epochStart    = long.MinValue; // lowest valid offset for current assignment; stale acks below this are discarded

        /// <summary>Records a consumed offset. Must be called before dispatching downstream.</summary>
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
                        // Backward seek: reset HWM, acked set, and phantom-gap queue.
                        // Advance epoch boundary to discard in-flight acks from the old position.
                        _lastCommitted = offset - 1;
                        _acked.Clear();
                        _phantomGaps.Clear();
                        _epochStart = offset;
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
            }
        }

        /// <summary>
        /// Marks <paramref name="offset"/> as acknowledged. Returns <c>hwm + 1</c> (Kafka commit convention)
        /// when a contiguous run is complete; otherwise <see langword="null"/>.
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

                // Discard stale acks from a previous epoch; they'd cause a memory leak or spurious HWM advance.
                if (offset < _epochStart)
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

