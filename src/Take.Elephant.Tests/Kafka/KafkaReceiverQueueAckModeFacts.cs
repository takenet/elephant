using System;
using System.Collections.Generic;
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

            var queue = new KafkaReceiverQueue<TestItem>(consumer, serializer, "test-topic", ackMode);
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
            // After ack, StoreOffset should be called with offset 11 (10+1)
            consumer.Received(1).StoreOffset(
                Arg.Is<TopicPartitionOffset>(tpo => tpo.Offset == new Offset(11)));

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

            // StoreOffset and Commit must be called exactly once
            consumer.Received(1).StoreOffset(Arg.Any<TopicPartitionOffset>());
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

            // Do NOT ack
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
            consumer.Received(1).StoreOffset(
                Arg.Is<TopicPartitionOffset>(tpo => tpo.Offset == new Offset(101)));

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
            consumer.Received(1).StoreOffset(Arg.Is<TopicPartitionOffset>(tpo => tpo.Offset == new Offset(1)));

            await msg2.AcknowledgeAsync();
            // Commit should be at offset=2 (1+1)
            consumer.Received(1).StoreOffset(Arg.Is<TopicPartitionOffset>(tpo => tpo.Offset == new Offset(2)));

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

            // Now ack msg1 — fills the gap, HWM advances to offset 1, commits at offset=2
            await msg1.AcknowledgeAsync();
            consumer.Received(1).StoreOffset(Arg.Is<TopicPartitionOffset>(tpo => tpo.Offset == new Offset(2)));

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

        public sealed class TestItem
        {
            public string Value { get; set; }
        }
    }
}
