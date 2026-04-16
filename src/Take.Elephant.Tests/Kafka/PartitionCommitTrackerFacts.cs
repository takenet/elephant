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
