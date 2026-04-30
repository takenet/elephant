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
            tracker.Track(0);

            var result = tracker.Acknowledge(0);

            Assert.Equal(1L, result);
        }

        // ─── In-order sequential ──────────────────────────────────────────────────

        [Fact]
        public void Acknowledge_InOrder_AdvancesHWMStepByStep()
        {
            var tracker = new PartitionCommitTracker();
            tracker.Track(0);
            tracker.Track(1);
            tracker.Track(2);

            Assert.Equal(1L, tracker.Acknowledge(0));
            Assert.Equal(2L, tracker.Acknowledge(1));
            Assert.Equal(3L, tracker.Acknowledge(2));
        }

        // ─── Out-of-order / gap blocking ──────────────────────────────────────────

        [Fact]
        public void Acknowledge_OutOfOrder_HoldsUntilGapFilled()
        {
            var tracker = new PartitionCommitTracker();
            tracker.Track(0);
            tracker.Track(1);
            tracker.Track(2);

            // Ack 1 and 2 first — gap at 0 blocks advance
            Assert.Null(tracker.Acknowledge(1));
            Assert.Null(tracker.Acknowledge(2));

            // Fill gap at 0 — HWM bulk-advances to 2, commit at 3
            Assert.Equal(3L, tracker.Acknowledge(0));
        }

        // ─── Partial advance then batch ───────────────────────────────────────────

        [Fact]
        public void Acknowledge_GapInMiddle_PartialAdvanceThenBatch()
        {
            var tracker = new PartitionCommitTracker();
            tracker.Track(0);
            tracker.Track(1);
            tracker.Track(2);
            tracker.Track(3);

            Assert.Equal(1L, tracker.Acknowledge(0));  // HWM=0, commit 1
            Assert.Null(tracker.Acknowledge(2));        // gap at 1
            Assert.Equal(3L, tracker.Acknowledge(1));  // fills gap → HWM=2, commit 3
            Assert.Equal(4L, tracker.Acknowledge(3));  // HWM=3, commit 4
        }

        // ─── Defensive bootstrap (Acknowledge without prior Track) ────────────────

        [Fact]
        public void Acknowledge_WithoutTrack_DefensiveBootstrap_CommitsFromOffset()
        {
            var tracker = new PartitionCommitTracker();

            var result = tracker.Acknowledge(5);

            Assert.Equal(6L, result);
        }

        // ─── Non-zero starting offset ─────────────────────────────────────────────

        [Fact]
        public void Track_NonZeroStart_HWMRelativeToFirstOffset()
        {
            var tracker = new PartitionCommitTracker();
            tracker.Track(100);
            tracker.Track(101);

            Assert.Equal(101L, tracker.Acknowledge(100));
            Assert.Equal(102L, tracker.Acknowledge(101));
        }

        // ─── Seek / intra-assignment offset discontinuity ─────────────────────────

        [Fact]
        public void Track_BackwardSeek_ResetsHWMToNewBase()
        {
            // Consumption starts at 50.
            var tracker = new PartitionCommitTracker();
            tracker.Track(50);
            tracker.Track(51);
            tracker.Acknowledge(50);    // HWM = 50, next expected = 51

            // Seek backward to offset 10 (replaying old messages).
            // 10 < 51 → backward branch → reset HWM, acked set, and phantom-gap queue.
            tracker.Track(10);

            // Ack 10 must commit at 11, not get confused by the previous HWM.
            Assert.Equal(11L, tracker.Acknowledge(10));
        }

        // ─── Forward gaps (compaction / aborted transactions) ─────────────────────

        [Fact]
        public void Track_ForwardGap_WithInflight_PhantomRangeSkipped_HWMBlockedUntilPriorAcked()
        {
            // Scenario from reviewer: offsets 0 and 1 are in flight;
            // offset 3 arrives because offset 2 was compacted.
            // The tracker must NOT commit until both 0 and 1 are acked.
            var tracker = new PartitionCommitTracker();
            tracker.Track(0);
            tracker.Track(1);
            tracker.Track(3); // forward gap: phantom range (2,2) enqueued — O(1), no loop

            // Ack 3 first (fast handler) — HWM cannot advance past 0's pending slot.
            Assert.Null(tracker.Acknowledge(3));

            // Ack 0 — HWM advances to 0, but stops because 1 is still pending.
            Assert.Equal(1L, tracker.Acknowledge(0));

            // Ack 1 — fills the real gap; the phantom range (2,2) is dequeued in one step;
            // HWM bulk-advances through 1 → phantom 2 → 3 → commit at 4.
            Assert.Equal(4L, tracker.Acknowledge(1));
        }

        [Fact]
        public void Track_ForwardGap_WithoutInflight_AdvancesHWMInO1()
        {
            // All prior offsets are committed before the gap arrives.
            // No phantom loop needed — _lastCommitted jumps directly.
            var tracker = new PartitionCommitTracker();
            tracker.Track(0);
            tracker.Track(1);
            tracker.Track(2);
            Assert.Equal(1L, tracker.Acknowledge(0));
            Assert.Equal(2L, tracker.Acknowledge(1));
            Assert.Equal(3L, tracker.Acknowledge(2)); // _lastCommitted==2 == _lastTracked==2, _acked={}

            // Gap: offsets 3–99 were compacted; 100 is the next delivered offset.
            tracker.Track(100); // forward gap, no in-flight → O(1) advance

            // Ack 100 should commit at 101 immediately (no phantom loop necessary).
            Assert.Equal(101L, tracker.Acknowledge(100));
        }

        [Fact]
        public void Track_ForwardGap_WithInflight_PhantomRangeDequeued_NoMemoryLeak()
        {
            // The phantom range must be removed from the queue as the HWM advances through it.
            // This test verifies that the queue does not retain phantom ranges after the HWM passes.
            var tracker = new PartitionCommitTracker();
            tracker.Track(0);
            tracker.Track(5); // forward gap with 0 still in flight → phantom range (1,4) enqueued

            // Ack 5 first — HWM stuck at -1 (waiting for 0).
            Assert.Null(tracker.Acknowledge(5));

            // Ack 0 — HWM advances: 0 → phantom range (1,4) dequeued (skip to 4) → 5 → stop. Commit at 6.
            Assert.Equal(6L, tracker.Acknowledge(0));

            // Subsequent track/ack at 6 confirms no leftover state in _acked.
            tracker.Track(6);
            Assert.Equal(7L, tracker.Acknowledge(6));
        }

        [Fact]
        public void Track_MultipleForwardGaps_EachGapHandledCorrectly()
        {
            // Two separate compaction gaps in the same assignment.
            var tracker = new PartitionCommitTracker();
            tracker.Track(0);
            Assert.Equal(1L, tracker.Acknowledge(0)); // _lastCommitted=0, _lastTracked=0

            // Gap 1: offsets 1-4 compacted, 5 arrives.
            // _acked={}, _lastCommitted(0)==_lastTracked(0) → O(1): _lastCommitted=4
            tracker.Track(5);

            // Ack 5: fills _acked, no gap → commit at 6.
            Assert.Equal(6L, tracker.Acknowledge(5)); // _lastCommitted=5, _lastTracked=5

            // Gap 2: offsets 6-9 compacted, 10 arrives.
            // _acked={}, _lastCommitted(5)==_lastTracked(5) → O(1): _lastCommitted=9
            tracker.Track(10);

            Assert.Equal(11L, tracker.Acknowledge(10));
        }

        [Fact]
        public void Track_BackwardSeek_StaleAcksBeforeEpochStart_Discarded()
        {
            // After a backward seek to offset 50, acks for offsets below 50 are discarded.
            var tracker = new PartitionCommitTracker();
            tracker.Track(100);
            tracker.Track(101);

            // Seek backward to 50.
            tracker.Track(50); // backward: reset, _epochStart=50

            // Ack for offset 40 (below epochStart=50) must be discarded.
            Assert.Null(tracker.Acknowledge(40));

            // Ack for the new epoch works normally.
            Assert.Equal(51L, tracker.Acknowledge(50));
        }

        [Fact]
        public void Track_MultipleBackwardSeeks_EachResetsIndependently()
        {
            var tracker = new PartitionCommitTracker();
            tracker.Track(100);

            // First backward seek to 50.
            tracker.Track(50); // 50 < 100 → reset, _epochStart=50

            // Second backward seek to 20.
            tracker.Track(20); // 20 < 50 → reset, _epochStart=20

            // Acks below the final epochStart(20) are discarded.
            Assert.Null(tracker.Acknowledge(10));

            // Current epoch starts at 20 — ack works.
            Assert.Equal(21L, tracker.Acknowledge(20));
        }

        [Fact]
        public void Track_BackwardSeek_OutOfOrderAcksAfterSeekWorkNormally()
        {
            var tracker = new PartitionCommitTracker();
            tracker.Track(100);

            // Seek backward to 10.
            tracker.Track(10); // backward: reset, _epochStart=10
            tracker.Track(11);
            tracker.Track(12);

            // Ack out of order: 12, 11, then 10.
            Assert.Null(tracker.Acknowledge(12));   // gap at 10 and 11
            Assert.Null(tracker.Acknowledge(11));   // gap at 10
            Assert.Equal(13L, tracker.Acknowledge(10)); // fills gap → bulk advance to 12
        }

        [Fact]
        public void Track_BackwardSeek_ClearsPhantomGaps()
        {
            // A backward seek must clear any pending phantom ranges, otherwise
            // a range recorded before the seek could interfere with the new epoch.
            var tracker = new PartitionCommitTracker();
            tracker.Track(0);
            tracker.Track(5); // forward gap with 0 in-flight → phantom range (1,4) enqueued

            // Seek backward to 10 (offset 10 > lastTracked=5 is a forward jump... use 3 instead)
            // Actually demonstrate with a backward seek that discards the phantom range:
            tracker.Track(0); // 0 < 5 → backward: resets HWM, acked, AND phantom-gap queue

            // After reset, phantom range (1,4) must be gone.
            // Ack 0 in the new epoch — should commit at 1 without any phantom interference.
            Assert.Equal(1L, tracker.Acknowledge(0));
        }

        // ─── Stale ack on old tracker instance (rebalance simulation) ─────────────

        [Fact]
        public void Acknowledge_OnOldInstance_DoesNotAffectNewInstance()
        {
            // Simulates what happens after a rebalance:
            // KafkaReceiverQueue replaces the tracker with a new instance.
            var oldTracker = new PartitionCommitTracker();
            oldTracker.Track(0);
            oldTracker.Track(1);

            // New assignment starts at offset 50
            var newTracker = new PartitionCommitTracker();
            newTracker.Track(50);

            // Old in-flight delegate acks offset 0 on the old instance
            var commitFromOld = oldTracker.Acknowledge(0);
            Assert.Equal(1L, commitFromOld); // old tracker advances normally

            // New tracker is completely unaffected
            Assert.Equal(51L, newTracker.Acknowledge(50));
        }
    }
}
