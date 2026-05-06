using Take.Elephant.Kafka;
using Xunit;

namespace Take.Elephant.Tests.Kafka
{
    [Trait("Category", nameof(Kafka))]
    public class PartitionCommitTrackerFacts
    {
        // ─── Single offset ────────────────────────────────────────────────────────

        [Fact]
        public void Acknowledge_SingleOffset_ReturnsNextOffset()
        {
            var tracker = new PartitionCommitTracker();
            var epoch = tracker.Track(0);

            var result = tracker.Acknowledge(0, epoch);

            Assert.Equal(1L, result);
        }

        // ─── In-order sequential ──────────────────────────────────────────────────

        [Fact]
        public void Acknowledge_InOrder_AdvancesHWMStepByStep()
        {
            var tracker = new PartitionCommitTracker();
            var e0 = tracker.Track(0);
            var e1 = tracker.Track(1);
            var e2 = tracker.Track(2);

            Assert.Equal(1L, tracker.Acknowledge(0, e0));
            Assert.Equal(2L, tracker.Acknowledge(1, e1));
            Assert.Equal(3L, tracker.Acknowledge(2, e2));
        }

        // ─── Out-of-order / gap blocking ──────────────────────────────────────────

        [Fact]
        public void Acknowledge_OutOfOrder_HoldsUntilGapFilled()
        {
            var tracker = new PartitionCommitTracker();
            var e0 = tracker.Track(0);
            var e1 = tracker.Track(1);
            var e2 = tracker.Track(2);

            // Ack 1 and 2 first — gap at 0 blocks advance
            Assert.Null(tracker.Acknowledge(1, e1));
            Assert.Null(tracker.Acknowledge(2, e2));

            // Fill gap at 0 — HWM bulk-advances to 2, commit at 3
            Assert.Equal(3L, tracker.Acknowledge(0, e0));
        }

        // ─── Partial advance then batch ───────────────────────────────────────────

        [Fact]
        public void Acknowledge_GapInMiddle_PartialAdvanceThenBatch()
        {
            var tracker = new PartitionCommitTracker();
            var e0 = tracker.Track(0);
            var e1 = tracker.Track(1);
            var e2 = tracker.Track(2);
            var e3 = tracker.Track(3);

            Assert.Equal(1L, tracker.Acknowledge(0, e0));  // HWM=0, commit 1
            Assert.Null(tracker.Acknowledge(2, e2));        // gap at 1
            Assert.Equal(3L, tracker.Acknowledge(1, e1));  // fills gap → HWM=2, commit 3
            Assert.Equal(4L, tracker.Acknowledge(3, e3));  // HWM=3, commit 4
        }

        // ─── Defensive bootstrap (Acknowledge without prior Track) ────────────────

        [Fact]
        public void Acknowledge_WithoutTrack_DefensiveBootstrap_CommitsFromOffset()
        {
            var tracker = new PartitionCommitTracker();

            // Epoch is 0 when no backward seek has occurred.
            var result = tracker.Acknowledge(5, 0L);

            Assert.Equal(6L, result);
        }

        // ─── Non-zero starting offset ─────────────────────────────────────────────

        [Fact]
        public void Track_NonZeroStart_HWMRelativeToFirstOffset()
        {
            var tracker = new PartitionCommitTracker();
            var e100 = tracker.Track(100);
            var e101 = tracker.Track(101);

            Assert.Equal(101L, tracker.Acknowledge(100, e100));
            Assert.Equal(102L, tracker.Acknowledge(101, e101));
        }

        // ─── Seek / intra-assignment offset discontinuity ─────────────────────────

        [Fact]
        public void Track_BackwardSeek_ResetsHWMToNewBase()
        {
            // Consumption starts at 50.
            var tracker = new PartitionCommitTracker();
            var e50 = tracker.Track(50);
            tracker.Track(51);
            tracker.Acknowledge(50, e50);   // HWM = 50, next expected = 51

            // Seek backward to offset 10 (replaying old messages).
            // 10 < 51 → backward branch → reset HWM, acked set, phantom-gap queue, and advance epoch.
            var e10 = tracker.Track(10);

            // Ack 10 must commit at 11, not get confused by the previous HWM.
            Assert.Equal(11L, tracker.Acknowledge(10, e10));
        }

        // ─── Forward gaps (compaction / aborted transactions) ─────────────────────

        [Fact]
        public void Track_ForwardGap_WithInflight_PhantomRangeSkipped_HWMBlockedUntilPriorAcked()
        {
            // Scenario from reviewer: offsets 0 and 1 are in flight;
            // offset 3 arrives because offset 2 was compacted.
            // The tracker must NOT commit until both 0 and 1 are acked.
            var tracker = new PartitionCommitTracker();
            var e0 = tracker.Track(0);
            var e1 = tracker.Track(1);
            var e3 = tracker.Track(3); // forward gap: phantom range (2,2) enqueued — O(1), no loop

            // Ack 3 first (fast handler) — HWM cannot advance past 0's pending slot.
            Assert.Null(tracker.Acknowledge(3, e3));

            // Ack 0 — HWM advances to 0, but stops because 1 is still pending.
            Assert.Equal(1L, tracker.Acknowledge(0, e0));

            // Ack 1 — fills the real gap; the phantom range (2,2) is dequeued in one step;
            // HWM bulk-advances through 1 → phantom 2 → 3 → commit at 4.
            Assert.Equal(4L, tracker.Acknowledge(1, e1));
        }

        [Fact]
        public void Track_ForwardGap_WithoutInflight_AdvancesHWMInO1()
        {
            // All prior offsets are committed before the gap arrives.
            // No phantom loop needed — _lastCommitted jumps directly.
            var tracker = new PartitionCommitTracker();
            var e0 = tracker.Track(0);
            var e1 = tracker.Track(1);
            var e2 = tracker.Track(2);
            Assert.Equal(1L, tracker.Acknowledge(0, e0));
            Assert.Equal(2L, tracker.Acknowledge(1, e1));
            Assert.Equal(3L, tracker.Acknowledge(2, e2)); // _lastCommitted==2 == _lastTracked==2, _acked={}

            // Gap: offsets 3–99 were compacted; 100 is the next delivered offset.
            var e100 = tracker.Track(100); // forward gap, no in-flight → O(1) advance

            // Ack 100 should commit at 101 immediately (no phantom loop necessary).
            Assert.Equal(101L, tracker.Acknowledge(100, e100));
        }

        [Fact]
        public void Track_ForwardGap_WithInflight_PhantomRangeDequeued_NoMemoryLeak()
        {
            // The phantom range must be removed from the queue as the HWM advances through it.
            // This test verifies that the queue does not retain phantom ranges after the HWM passes.
            var tracker = new PartitionCommitTracker();
            var e0 = tracker.Track(0);
            var e5 = tracker.Track(5); // forward gap with 0 still in flight → phantom range (1,4) enqueued

            // Ack 5 first — HWM stuck at -1 (waiting for 0).
            Assert.Null(tracker.Acknowledge(5, e5));

            // Ack 0 — HWM advances: 0 → phantom range (1,4) dequeued (skip to 4) → 5 → stop. Commit at 6.
            Assert.Equal(6L, tracker.Acknowledge(0, e0));

            // Subsequent track/ack at 6 confirms no leftover state in _acked.
            var e6 = tracker.Track(6);
            Assert.Equal(7L, tracker.Acknowledge(6, e6));
        }

        [Fact]
        public void Track_MultipleForwardGaps_EachGapHandledCorrectly()
        {
            // Two separate compaction gaps in the same assignment.
            var tracker = new PartitionCommitTracker();
            var e0 = tracker.Track(0);
            Assert.Equal(1L, tracker.Acknowledge(0, e0)); // _lastCommitted=0, _lastTracked=0

            // Gap 1: offsets 1-4 compacted, 5 arrives.
            // _acked={}, _lastCommitted(0)==_lastTracked(0) → O(1): _lastCommitted=4
            var e5 = tracker.Track(5);

            // Ack 5: fills _acked, no gap → commit at 6.
            Assert.Equal(6L, tracker.Acknowledge(5, e5)); // _lastCommitted=5, _lastTracked=5

            // Gap 2: offsets 6-9 compacted, 10 arrives.
            // _acked={}, _lastCommitted(5)==_lastTracked(5) → O(1): _lastCommitted=9
            var e10 = tracker.Track(10);

            Assert.Equal(11L, tracker.Acknowledge(10, e10));
        }

        [Fact]
        public void Track_BackwardSeek_StaleAcksFromOldEpoch_Discarded()
        {
            // After a backward seek, acks carrying the old epoch id are rejected.
            var tracker = new PartitionCommitTracker();
            var staleEpoch = tracker.Track(100);
            tracker.Track(101);

            // Seek backward to 50 — epoch is incremented.
            var newEpoch = tracker.Track(50); // backward: reset, epoch advances

            // Ack from the old epoch (below new epochStart=50) must be discarded.
            Assert.Null(tracker.Acknowledge(40, staleEpoch));

            // Ack in the new epoch works normally.
            Assert.Equal(51L, tracker.Acknowledge(50, newEpoch));
        }

        [Fact]
        public void Track_BackwardSeek_StaleAckAtOverlappingOffset_Discarded()
        {
            // Key scenario: an in-flight ack delegate holds an offset that falls WITHIN the
            // new epoch's range (offset >= new epochStart), but was tracked in the old epoch.
            // The old _epochStart check alone would NOT catch this; only epoch-id validation does.
            var tracker = new PartitionCommitTracker();
            tracker.Track(50);
            var staleEpoch55 = tracker.Track(55); // epoch 0, offset 55

            // Backward seek to 40 → epochStart=40, epoch incremented to 1.
            var newEpoch40 = tracker.Track(40);

            // Stale ack for offset 55 (epoch 0): 55 >= epochStart(40) so the old offset-comparison
            // guard would have accepted it — the epoch-id check correctly rejects it.
            Assert.Null(tracker.Acknowledge(55, staleEpoch55));

            // Legitimate ack in the new epoch commits correctly.
            Assert.Equal(41L, tracker.Acknowledge(40, newEpoch40));
        }

        [Fact]
        public void Track_MultipleBackwardSeeks_EachResetsIndependently()
        {
            var tracker = new PartitionCommitTracker();
            tracker.Track(100);                       // epoch 0

            // First backward seek to 50.
            tracker.Track(50); // 50 < 100 → reset, epoch becomes 1

            // Second backward seek to 20.
            tracker.Track(20); // 20 < 50 → reset, epoch becomes 2

            // Acks from superseded epochs are discarded.
            Assert.Null(tracker.Acknowledge(10, 0L)); // epoch 0 — stale
            Assert.Null(tracker.Acknowledge(10, 1L)); // epoch 1 — stale

            // Current epoch is 2; offset 20 was the last tracked after the final seek.
            Assert.Equal(21L, tracker.Acknowledge(20, 2L));
        }

        [Fact]
        public void Track_BackwardSeek_OutOfOrderAcksAfterSeekWorkNormally()
        {
            var tracker = new PartitionCommitTracker();
            tracker.Track(100); // epoch 0

            // Seek backward to 10.
            var e10 = tracker.Track(10); // backward: epoch = 1
            var e11 = tracker.Track(11);
            var e12 = tracker.Track(12);

            // Ack out of order: 12, 11, then 10.
            Assert.Null(tracker.Acknowledge(12, e12));   // gap at 10 and 11
            Assert.Null(tracker.Acknowledge(11, e11));   // gap at 10
            Assert.Equal(13L, tracker.Acknowledge(10, e10)); // fills gap → bulk advance to 12
        }

        [Fact]
        public void Track_BackwardSeek_ClearsPhantomGaps()
        {
            // A backward seek must clear any pending phantom ranges, otherwise
            // a range recorded before the seek could interfere with the new epoch.
            var tracker = new PartitionCommitTracker();
            tracker.Track(0);
            tracker.Track(5); // forward gap with 0 in-flight → phantom range (1,4) enqueued

            // Backward seek (0 < 5) discards the phantom range.
            var e0 = tracker.Track(0); // 0 < 5 → backward: resets HWM, acked, AND phantom-gap queue

            // After reset, phantom range (1,4) must be gone.
            // Ack 0 in the new epoch — should commit at 1 without any phantom interference.
            Assert.Equal(1L, tracker.Acknowledge(0, e0));
        }

        // ─── Duplicate / already-committed acks ──────────────────────────────────

        [Fact]
        public void Acknowledge_DuplicateAck_DoesNotGrowAckedSet()
        {
            // Acking an already-committed offset must be a no-op (returns null, no memory leak).
            var tracker = new PartitionCommitTracker();
            var e0 = tracker.Track(0);
            var e1 = tracker.Track(1);

            Assert.Equal(1L, tracker.Acknowledge(0, e0)); // commits offset 0
            Assert.Equal(2L, tracker.Acknowledge(1, e1)); // commits offset 1

            // Duplicate ack of offset 0 — already committed, must be silently discarded.
            Assert.Null(tracker.Acknowledge(0, e0));
        }

        // ─── Stale ack on old tracker instance (rebalance simulation) ─────────────

        [Fact]
        public void Acknowledge_OnOldInstance_DoesNotAffectNewInstance()
        {
            // Simulates what happens after a rebalance:
            // KafkaReceiverQueue replaces the tracker with a new instance.
            var oldTracker = new PartitionCommitTracker();
            var oe0 = oldTracker.Track(0);
            oldTracker.Track(1);

            // New assignment starts at offset 50
            var newTracker = new PartitionCommitTracker();
            var ne50 = newTracker.Track(50);

            // Old in-flight delegate acks offset 0 on the old instance
            var commitFromOld = oldTracker.Acknowledge(0, oe0);
            Assert.Equal(1L, commitFromOld); // old tracker advances normally

            // New tracker is completely unaffected
            Assert.Equal(51L, newTracker.Acknowledge(50, ne50));
        }
    }
}
