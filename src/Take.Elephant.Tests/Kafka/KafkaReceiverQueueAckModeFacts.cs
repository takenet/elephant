using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using NSubstitute;
using Take.Elephant.Kafka;
using Xunit;

namespace Take.Elephant.Tests.Kafka
{
    [Trait("Category", nameof(Kafka))]
    public class KafkaReceiverQueueAckModeFacts
    {
        // ─── Helpers ──────────────────────────────────────────────────────────────

        private static (IConsumer<Ignore, string> Consumer, ISerializer<TestItem> Serializer, KafkaReceiverQueue<TestItem> Queue)
            BuildQueue(KafkaAckMode ackMode, params ConsumeResult<Ignore, string>[] sequence)
        {
            var serializer = Substitute.For<ISerializer<TestItem>>();
            var consumer = Substitute.For<IConsumer<Ignore, string>>();
            consumer.Subscription.Returns(new List<string>());

            SetupConsumeSequence(consumer, sequence);

            // Non-Eager modes require the internal constructor (test seam) because the public
            // constructor enforces Eager-only for injected consumers.
            var queue = ackMode == KafkaAckMode.Eager
                ? new KafkaReceiverQueue<TestItem>(consumer, serializer, "test-topic", ackMode)
                : new KafkaReceiverQueue<TestItem>(consumer, serializer, "test-topic", ackMode,
                    new ConcurrentDictionary<TopicPartition, PartitionCommitTracker>());
            return (consumer, serializer, queue);
        }

        private static ConsumeResult<Ignore, string> MakeResult(string serializedValue, int partition = 0, long offset = 0)
        {
            return new ConsumeResult<Ignore, string>
            {
                Topic = "test-topic",
                Partition = new Partition(partition),
                Offset = new Offset(offset),
                Message = new Message<Ignore, string>
                {
                    Value = serializedValue,
                    Headers = new Headers()
                }
            };
        }

        private static void SetupConsumeSequence(IConsumer<Ignore, string> consumer, ConsumeResult<Ignore, string>[] sequence)
        {
            var calls = 0;
            consumer
                .Consume(Arg.Any<CancellationToken>())
                .Returns(callInfo =>
                {
                    var ct = callInfo.Arg<CancellationToken>();
                    if (calls < sequence.Length)
                        return sequence[calls++];

                    ct.WaitHandle.WaitOne();
                    throw new OperationCanceledException(ct);
                });
        }

        // ─── Eager mode (legacy) ─────────────────────────────────────────────────

        [Fact]
        public async Task EagerMode_DequeueAsync_ReturnsItem_WithoutExplicitAck()
        {
            var item = new TestItem { Value = "eager-payload" };
            var result = MakeResult("ser-eager");
            var (_, serializer, queue) = BuildQueue(KafkaAckMode.Eager, result);
            serializer.Deserialize("ser-eager").Returns(item);

            var dequeued = await queue.DequeueAsync(CancellationToken.None);

            Assert.Same(item, dequeued);
            await queue.CloseAsync(CancellationToken.None);
        }

        [Fact]
        public async Task EagerMode_DequeueAckableAsync_ThrowsInvalidOperationException()
        {
            var (_, _, queue) = BuildQueue(KafkaAckMode.Eager);

            await Assert.ThrowsAsync<InvalidOperationException>(
                () => queue.DequeueAckableAsync(CancellationToken.None));

            await queue.CloseAsync(CancellationToken.None);
        }

        [Fact]
        public async Task EagerMode_DequeueAckableOrDefaultAsync_ThrowsInvalidOperationException()
        {
            var (_, _, queue) = BuildQueue(KafkaAckMode.Eager);

            await Assert.ThrowsAsync<InvalidOperationException>(
                () => queue.DequeueAckableOrDefaultAsync(CancellationToken.None));

            await queue.CloseAsync(CancellationToken.None);
        }

        // ─── OnSuccess mode ───────────────────────────────────────────────────────

        [Fact]
        public async Task OnSuccessMode_DequeueAckableAsync_ReturnsAckableMessage()
        {
            var item = new TestItem { Value = "on-success" };
            var result = MakeResult("ser-ok", partition: 0, offset: 5);
            var (_, serializer, queue) = BuildQueue(KafkaAckMode.OnSuccess, result);
            serializer.Deserialize("ser-ok").Returns(item);

            var msg = await queue.DequeueAckableAsync(CancellationToken.None);

            Assert.Same(item, msg.Item);
            Assert.Equal("test-topic", msg.Topic);
            Assert.Equal(0, msg.Partition);
            Assert.Equal(5L, msg.Offset);
            Assert.False(msg.IsAcknowledged);

            await queue.CloseAsync(CancellationToken.None);
        }

        [Fact]
        public async Task OnSuccessMode_AcknowledgeAsync_CommitsOffset()
        {
            var item = new TestItem { Value = "on-success-ack" };
            var result = MakeResult("ser-ack", partition: 1, offset: 10);
            var (consumer, serializer, queue) = BuildQueue(KafkaAckMode.OnSuccess, result);
            serializer.Deserialize("ser-ack").Returns(item);

            var msg = await queue.DequeueAckableAsync(CancellationToken.None);
            await msg.AcknowledgeAsync();

            Assert.True(msg.IsAcknowledged);
            // After ack, Commit should be called with offset 11 (10+1, per Kafka convention).
            // StoreOffset is no longer called: Commit(explicit) bypasses the local offset store.
            consumer.Received(1).Commit(
                Arg.Is<IEnumerable<TopicPartitionOffset>>(tpos =>
                    tpos.Any(tpo => tpo.Offset == new Offset(11))));

            await queue.CloseAsync(CancellationToken.None);
        }

        [Fact]
        public async Task OnSuccessMode_AcknowledgeAsync_IsIdempotent()
        {
            var item = new TestItem { Value = "idempotent" };
            var result = MakeResult("ser-idem", partition: 0, offset: 7);
            var (consumer, serializer, queue) = BuildQueue(KafkaAckMode.OnSuccess, result);
            serializer.Deserialize("ser-idem").Returns(item);

            var msg = await queue.DequeueAckableAsync(CancellationToken.None);

            // Invoke ack twice
            await msg.AcknowledgeAsync();
            await msg.AcknowledgeAsync();

            // Commit must be called exactly once; StoreOffset is no longer called.
            consumer.DidNotReceive().StoreOffset(Arg.Any<TopicPartitionOffset>());
            consumer.Received(1).Commit(Arg.Any<IEnumerable<TopicPartitionOffset>>());

            await queue.CloseAsync(CancellationToken.None);
        }

        [Fact]
        public async Task OnSuccessMode_NoCommit_BeforeAcknowledge()
        {
            var item = new TestItem { Value = "no-commit" };
            var result = MakeResult("ser-nc", partition: 0, offset: 3);
            var (consumer, serializer, queue) = BuildQueue(KafkaAckMode.OnSuccess, result);
            serializer.Deserialize("ser-nc").Returns(item);

            var msg = await queue.DequeueAckableAsync(CancellationToken.None);

            // Do NOT ack — neither StoreOffset nor Commit should be called.
            consumer.DidNotReceive().StoreOffset(Arg.Any<TopicPartitionOffset>());
            consumer.DidNotReceive().Commit(Arg.Any<IEnumerable<TopicPartitionOffset>>());

            await queue.CloseAsync(CancellationToken.None);
        }

        // ─── Manual mode ──────────────────────────────────────────────────────────

        [Fact]
        public async Task ManualMode_BehavesLikeOnSuccess_ForAck()
        {
            var item = new TestItem { Value = "manual" };
            var result = MakeResult("ser-man", partition: 2, offset: 100);
            var (consumer, serializer, queue) = BuildQueue(KafkaAckMode.Manual, result);
            serializer.Deserialize("ser-man").Returns(item);

            var msg = await queue.DequeueAckableAsync(CancellationToken.None);
            await msg.AcknowledgeAsync();

            Assert.True(msg.IsAcknowledged);
            consumer.DidNotReceive().StoreOffset(Arg.Any<TopicPartitionOffset>());
            consumer.Received(1).Commit(
                Arg.Is<IEnumerable<TopicPartitionOffset>>(tpos =>
                    tpos.Any(tpo => tpo.Offset == new Offset(101))));

            await queue.CloseAsync(CancellationToken.None);
        }

        // ─── PartitionCommitTracker in-order contiguous ───────────────────────────

        [Fact]
        public async Task OnSuccessMode_TwoMessagesInOrder_BothCommitSequentially()
        {
            var item1 = new TestItem { Value = "msg1" };
            var item2 = new TestItem { Value = "msg2" };
            var (consumer, serializer, queue) = BuildQueue(
                KafkaAckMode.OnSuccess,
                MakeResult("ser1", partition: 0, offset: 0),
                MakeResult("ser2", partition: 0, offset: 1));
            serializer.Deserialize("ser1").Returns(item1);
            serializer.Deserialize("ser2").Returns(item2);

            var msg1 = await queue.DequeueAckableAsync(CancellationToken.None);
            var msg2 = await queue.DequeueAckableAsync(CancellationToken.None);

            // Ack in order
            await msg1.AcknowledgeAsync();
            // Commit should be at offset=1 (0+1)
            consumer.Received(1).Commit(
                Arg.Is<IEnumerable<TopicPartitionOffset>>(tpos =>
                    tpos.Any(tpo => tpo.Offset == new Offset(1))));

            await msg2.AcknowledgeAsync();
            // Commit should be at offset=2 (1+1)
            consumer.Received(1).Commit(
                Arg.Is<IEnumerable<TopicPartitionOffset>>(tpos =>
                    tpos.Any(tpo => tpo.Offset == new Offset(2))));

            await queue.CloseAsync(CancellationToken.None);
        }

        [Fact]
        public async Task OnSuccessMode_TwoMessagesOutOfOrder_HWMAdvancesOnlyAfterGapFilled()
        {
            var item1 = new TestItem { Value = "msg1" };
            var item2 = new TestItem { Value = "msg2" };
            var (consumer, serializer, queue) = BuildQueue(
                KafkaAckMode.OnSuccess,
                MakeResult("ser1", partition: 0, offset: 0),
                MakeResult("ser2", partition: 0, offset: 1));
            serializer.Deserialize("ser1").Returns(item1);
            serializer.Deserialize("ser2").Returns(item2);

            var msg1 = await queue.DequeueAckableAsync(CancellationToken.None);
            var msg2 = await queue.DequeueAckableAsync(CancellationToken.None);

            // Ack msg2 first (out-of-order) — should NOT commit because offset 0 is still pending
            await msg2.AcknowledgeAsync();
            consumer.DidNotReceive().StoreOffset(Arg.Any<TopicPartitionOffset>());
            consumer.DidNotReceive().Commit(Arg.Any<IEnumerable<TopicPartitionOffset>>());

            // Now ack msg1 — fills the gap, HWM advances to offset 1, commits at offset=2
            await msg1.AcknowledgeAsync();
            consumer.DidNotReceive().StoreOffset(Arg.Any<TopicPartitionOffset>());
            consumer.Received(1).Commit(
                Arg.Is<IEnumerable<TopicPartitionOffset>>(tpos =>
                    tpos.Any(tpo => tpo.Offset == new Offset(2))));

            await queue.CloseAsync(CancellationToken.None);
        }

        // ─── OnSuccess mode: non-ackable Dequeue methods throw ───────────────────

        [Fact]
        public async Task OnSuccessMode_DequeueAsync_ThrowsInvalidOperationException()
        {
            var (_, _, queue) = BuildQueue(KafkaAckMode.OnSuccess);

            await Assert.ThrowsAsync<InvalidOperationException>(
                () => queue.DequeueAsync(CancellationToken.None));

            await queue.CloseAsync(CancellationToken.None);
        }

        [Fact]
        public async Task OnSuccessMode_DequeueOrDefaultAsync_ThrowsInvalidOperationException()
        {
            var (_, _, queue) = BuildQueue(KafkaAckMode.OnSuccess);

            await Assert.ThrowsAsync<InvalidOperationException>(
                () => queue.DequeueOrDefaultAsync(CancellationToken.None));

            await queue.CloseAsync(CancellationToken.None);
        }

        [Fact]
        public async Task OnSuccessMode_DequeueWithHeadersAsync_ThrowsInvalidOperationException()
        {
            var (_, _, queue) = BuildQueue(KafkaAckMode.OnSuccess);

            await Assert.ThrowsAsync<InvalidOperationException>(
                () => queue.DequeueWithHeadersAsync(CancellationToken.None));

            await queue.CloseAsync(CancellationToken.None);
        }

        [Fact]
        public async Task OnSuccessMode_DequeueWithHeadersOrDefaultAsync_ThrowsInvalidOperationException()
        {
            var (_, _, queue) = BuildQueue(KafkaAckMode.OnSuccess);

            await Assert.ThrowsAsync<InvalidOperationException>(
                () => queue.DequeueWithHeadersOrDefaultAsync(CancellationToken.None));

            await queue.CloseAsync(CancellationToken.None);
        }

        // ─── DequeueAckable* guards run before consumer is started (C6/C7) ─────────

        [Fact]
        public async Task EagerMode_DequeueAckableAsync_ThrowsBeforeStartingConsumer()
        {
            // Guard must fire synchronously (before any await inside) so the consumer
            // loop is never started as a side-effect of a misuse call.
            var (consumer, _, queue) = BuildQueue(KafkaAckMode.Eager);

            await Assert.ThrowsAsync<InvalidOperationException>(
                () => queue.DequeueAckableAsync(CancellationToken.None));

            // If the consumer task had been started, Consume() would have been called.
            consumer.DidNotReceive().Consume(Arg.Any<CancellationToken>());

            await queue.CloseAsync(CancellationToken.None);
        }

        [Fact]
        public async Task EagerMode_DequeueAckableOrDefaultAsync_ThrowsBeforeStartingConsumer()
        {
            var (consumer, _, queue) = BuildQueue(KafkaAckMode.Eager);

            await Assert.ThrowsAsync<InvalidOperationException>(
                () => queue.DequeueAckableOrDefaultAsync(CancellationToken.None));

            consumer.DidNotReceive().Consume(Arg.Any<CancellationToken>());

            await queue.CloseAsync(CancellationToken.None);
        }

        // ─── Ack delegate swallows disposal exceptions during shutdown (C2) ────────

        [Fact]
        public async Task OnSuccessMode_AcknowledgeAsync_AfterClose_DoesNotThrow()
        {
            // Simulates a message that was dequeued and is still in-flight when
            // CloseAsync is called. The ack delegate must swallow any disposal
            // exception rather than surfacing it to the caller.
            var item = new TestItem { Value = "in-flight" };
            var result = MakeResult("ser-in-flight", partition: 0, offset: 0);
            var (consumer, serializer, queue) = BuildQueue(KafkaAckMode.OnSuccess, result);
            serializer.Deserialize("ser-in-flight").Returns(item);

            // Make Commit throw ObjectDisposedException to simulate a closed consumer.
            consumer
                .When(c => c.Commit(Arg.Any<IEnumerable<TopicPartitionOffset>>()))
                .Do(_ => throw new ObjectDisposedException("_consumer"));

            await queue.OpenAsync(CancellationToken.None);
            var ackable = await queue.DequeueAckableAsync(CancellationToken.None);
            await queue.CloseAsync(CancellationToken.None);

            // Must not throw even though the consumer is now closed.
            var ex = await Record.ExceptionAsync(() => ackable.AcknowledgeAsync());
            Assert.Null(ex);
        }

        [Fact]
        public async Task OnSuccessMode_AcknowledgeAsync_BrokerException_IsRetryable()
        {
            // After a broker-level commit failure, IsAcknowledged must be false so
            // the caller can retry. On retry the cached commit offset is reused
            // (the tracker HWM has already advanced) and the second Commit() succeeds.
            var item = new TestItem { Value = "retry" };
            var tp = new TopicPartition("test-topic", new Partition(0));
            var result = MakeResult("ser-retry", partition: 0, offset: 0);
            var (consumer, serializer, queue, trackers) = BuildQueueWithTrackers(KafkaAckMode.OnSuccess, result);
            serializer.Deserialize("ser-retry").Returns(item);

            var tracker = new PartitionCommitTracker();
            trackers[tp] = tracker;

            // First Commit() call throws; second succeeds.
            var commitCallCount = 0;
            consumer
                .When(c => c.Commit(Arg.Any<IEnumerable<TopicPartitionOffset>>()))
                .Do(_ =>
                {
                    if (++commitCallCount == 1)
                        throw new KafkaException(new Error(ErrorCode.Local_AllBrokersDown));
                });

            var msg = await queue.DequeueAckableAsync(CancellationToken.None);

            // First ack fails — IsAcknowledged must remain false for retry.
            await Assert.ThrowsAsync<KafkaException>(() => msg.AcknowledgeAsync());
            Assert.False(msg.IsAcknowledged);

            // Retry: second ack must succeed and mark the message as acknowledged.
            await msg.AcknowledgeAsync();
            Assert.True(msg.IsAcknowledged);

            // Both attempts must have called Commit (second used cached commit offset).
            consumer.Received(2).Commit(Arg.Any<IEnumerable<TopicPartitionOffset>>());

            await queue.CloseAsync(CancellationToken.None);
        }

        // ─── Factory class — compile-time type safety ─────────────────────────────

        [Fact]
        public void Factory_CreateEager_ReturnsIKafkaReceiverQueue()
        {
            // The factory must return the narrowest interface for the requested mode.
            // CreateEager → IKafkaReceiverQueue<T>
            // CreateOnSuccess/CreateManual → IKafkaAckableReceiverQueue<T>
            // Because IKafkaAckableReceiverQueue<T> does NOT extend IKafkaReceiverQueue<T>,
            // assigning the result of CreateOnSuccess/CreateManual to IKafkaReceiverQueue<T>
            // is a compile-time error (CS0266) — the factory enforces this at the type level.
            var config = new ConsumerConfig { BootstrapServers = "localhost:9092", GroupId = "g" };
            var serializer = Substitute.For<ISerializer<TestItem>>();

            IKafkaReceiverQueue<TestItem> eager = KafkaReceiverQueue.CreateEager<TestItem>(config, "t", serializer);
            Assert.NotNull(eager);

            IKafkaAckableReceiverQueue<TestItem> onSuccess = KafkaReceiverQueue.CreateOnSuccess<TestItem>(config, "t", serializer);
            Assert.NotNull(onSuccess);

            IKafkaAckableReceiverQueue<TestItem> manual = KafkaReceiverQueue.CreateManual<TestItem>(config, "t", serializer);
            Assert.NotNull(manual);

            // The ackable references must NOT be implicitly castable to IKafkaReceiverQueue<T>.
            // (Verified at compile time by the explicit type annotations above — if the factory
            // returned IKafkaReceiverQueue<T>, the lines above would not compile.)

            (eager as IDisposable)?.Dispose();
            (onSuccess as IDisposable)?.Dispose();
            (manual as IDisposable)?.Dispose();
        }

        // ─── Concurrent ack ordering — monotonic commit ───────────────────────────

        [Fact]
        public async Task OnSuccessMode_TwoMessagesAckedConcurrently_CommitsAreMonotonic()
        {
            // Two messages on the same partition processed concurrently.
            // Regardless of which ack wins the per-partition commit lock, the monotonic
            // guard must ensure the broker cursor never regresses (a lower-offset commit
            // is suppressed once a higher-offset one has been issued for the same partition).
            var item1 = new TestItem { Value = "msg1" };
            var item2 = new TestItem { Value = "msg2" };
            var tp = new TopicPartition("test-topic", new Partition(0));
            var (consumer, serializer, queue, trackers) = BuildQueueWithTrackers(
                KafkaAckMode.OnSuccess,
                MakeResult("ser1", partition: 0, offset: 0),
                MakeResult("ser2", partition: 0, offset: 1));
            serializer.Deserialize("ser1").Returns(item1);
            serializer.Deserialize("ser2").Returns(item2);

            var tracker = new PartitionCommitTracker();
            trackers[tp] = tracker;

            var msg1 = await queue.DequeueAckableAsync(CancellationToken.None);
            var msg2 = await queue.DequeueAckableAsync(CancellationToken.None);

            // Ack both concurrently.
            await Task.WhenAll(
                Task.Run(() => msg1.AcknowledgeAsync()),
                Task.Run(() => msg2.AcknowledgeAsync()));

            Assert.True(msg1.IsAcknowledged);
            Assert.True(msg2.IsAcknowledged);

            // Collect committed offsets in the order they were issued.
            var commitOffsets = consumer.ReceivedCalls()
                .Where(c => c.GetMethodInfo().Name == nameof(IConsumer<Ignore, string>.Commit))
                .Select(c => c.GetArguments()[0] as IEnumerable<TopicPartitionOffset>)
                .Where(x => x != null)
                .SelectMany(x => x)
                .Select(tpo => tpo.Offset.Value)
                .ToList();

            Assert.NotEmpty(commitOffsets);
            // The highest committed offset must cover both messages (offsets 0 and 1 → commit at 2).
            Assert.Equal(2L, commitOffsets.Max());
            // No commit must regress (monotonically non-decreasing).
            for (int i = 1; i < commitOffsets.Count; i++)
                Assert.True(commitOffsets[i] >= commitOffsets[i - 1],
                    $"Commit regressed: offset {commitOffsets[i - 1]} → {commitOffsets[i]}");

            await queue.CloseAsync(CancellationToken.None);
        }

        // ─── Constructor contract ─────────────────────────────────────────────────

        [Theory]
        [InlineData(KafkaAckMode.OnSuccess)]
        [InlineData(KafkaAckMode.Manual)]
        public void PublicIConsumerConstructor_WhenNonEagerMode_ThrowsArgumentException(KafkaAckMode ackMode)
        {
            var consumer = Substitute.For<IConsumer<Ignore, string>>();
            var serializer = Substitute.For<ISerializer<TestItem>>();

            var ex = Assert.Throws<ArgumentException>(() =>
                new KafkaReceiverQueue<TestItem>(consumer, serializer, "test-topic", ackMode));

            Assert.Equal("ackMode", ex.ParamName);
        }

        // ─── KafkaException narrowing (Fix 2) ─────────────────────────────────────

        /// <summary>
        /// Returns the queue AND the underlying partition-tracker dictionary so that
        /// tests can simulate rebalance events by manipulating the dictionary directly.
        /// </summary>
        private static (IConsumer<Ignore, string> Consumer,
                        ISerializer<TestItem> Serializer,
                        KafkaReceiverQueue<TestItem> Queue,
                        ConcurrentDictionary<TopicPartition, PartitionCommitTracker> Trackers)
            BuildQueueWithTrackers(KafkaAckMode ackMode, params ConsumeResult<Ignore, string>[] sequence)
        {
            var serializer = Substitute.For<ISerializer<TestItem>>();
            var consumer = Substitute.For<IConsumer<Ignore, string>>();
            consumer.Subscription.Returns(new List<string>());
            SetupConsumeSequence(consumer, sequence);

            var trackers = new ConcurrentDictionary<TopicPartition, PartitionCommitTracker>();
            var queue = new KafkaReceiverQueue<TestItem>(consumer, serializer, "test-topic", ackMode, trackers);
            return (consumer, serializer, queue, trackers);
        }

        [Fact]
        public async Task OnSuccessMode_AcknowledgeAsync_BrokerException_Propagates()
        {
            // If the commit fails for a broker-level reason (e.g., all brokers down)
            // while the partition is still active, the exception must propagate to the
            // caller — it must NOT be silently swallowed.
            var item = new TestItem { Value = "broker-fail" };
            var tp = new TopicPartition("test-topic", new Partition(0));
            var result = MakeResult("ser-bf", partition: 0, offset: 0);
            var (consumer, serializer, queue, trackers) = BuildQueueWithTrackers(KafkaAckMode.OnSuccess, result);
            serializer.Deserialize("ser-bf").Returns(item);

            // Ensure the partition tracker is present in the dict (simulates active partition).
            // Must be set before DequeueAckableAsync because that call starts the consumer task,
            // which uses GetOrAdd — finding the entry here guarantees capturedTracker == tracker.
            var tracker = new PartitionCommitTracker();
            trackers[tp] = tracker;

            // Simulate a broker-level commit failure (partition still active).
            var brokerError = new Error(ErrorCode.Local_AllBrokersDown);
            consumer
                .When(c => c.Commit(Arg.Any<IEnumerable<TopicPartitionOffset>>()))
                .Do(_ => throw new KafkaException(brokerError));

            var msg = await queue.DequeueAckableAsync(CancellationToken.None);

            // The exception must surface to the caller.
            await Assert.ThrowsAsync<KafkaException>(() => msg.AcknowledgeAsync());

            await queue.CloseAsync(CancellationToken.None);
        }

        [Fact]
        public async Task OnSuccessMode_AcknowledgeAsync_PartitionRevoked_DoesNotThrow()
        {
            // If the partition was revoked (tracker removed from the dict) before
            // AcknowledgeAsync is called, the commit should be skipped entirely.
            var item = new TestItem { Value = "revoked" };
            var tp = new TopicPartition("test-topic", new Partition(0));
            var result = MakeResult("ser-rev", partition: 0, offset: 0);
            var (consumer, serializer, queue, trackers) = BuildQueueWithTrackers(KafkaAckMode.OnSuccess, result);
            serializer.Deserialize("ser-rev").Returns(item);

            // Ensure the partition tracker is present so ConsumeAsync creates the ackable message.
            var tracker = new PartitionCommitTracker();
            trackers[tp] = tracker;

            var msg = await queue.DequeueAckableAsync(CancellationToken.None);

            // Simulate rebalance: remove the tracker before the ack fires.
            trackers.TryRemove(tp, out _);

            // Must not throw even though Commit would fail.
            var ex = await Record.ExceptionAsync(() => msg.AcknowledgeAsync());
            Assert.Null(ex);

            // Commit must not have been attempted.
            consumer.DidNotReceive().Commit(Arg.Any<IEnumerable<TopicPartitionOffset>>());

            await queue.CloseAsync(CancellationToken.None);
        }

        public sealed class TestItem
        {
            public string Value { get; set; }
        }
    }
}
