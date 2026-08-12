using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using NSubstitute;
using Take.Elephant.Kafka;
using Xunit;

namespace Take.Elephant.Tests.Kafka
{
    [Trait("Category", nameof(Kafka))]
    public class KafkaBatchSenderQueueFacts
    {
        [Fact]
        public async Task EnqueueBatchAsync_ShouldCallPublishAsyncOncePerItem()
        {
            var publisher = Substitute.For<IEventStreamPublisher<Null, string>>();
            var serializer = Substitute.For<ISerializer<string>>();
            serializer.Serialize("a").Returns("serialized-a");
            serializer.Serialize("b").Returns("serialized-b");
            serializer.Serialize("c").Returns("serialized-c");

            var queue = new KafkaBatchSenderQueue<string>(publisher, serializer, "topic-batch");

            await queue.EnqueueBatchAsync(["a", "b", "c"], CancellationToken.None);

            await publisher.Received(1).PublishAsync(null, "serialized-a", CancellationToken.None);
            await publisher.Received(1).PublishAsync(null, "serialized-b", CancellationToken.None);
            await publisher.Received(1).PublishAsync(null, "serialized-c", CancellationToken.None);
            await publisher.Received(3).PublishAsync(null, Arg.Any<string>(), CancellationToken.None);
        }

        [Fact]
        public async Task EnqueueBatchAsync_WithEmptyCollection_ShouldCompleteWithoutPublishing()
        {
            var publisher = Substitute.For<IEventStreamPublisher<Null, string>>();
            var serializer = Substitute.For<ISerializer<string>>();

            var queue = new KafkaBatchSenderQueue<string>(publisher, serializer, "topic-batch");

            await queue.EnqueueBatchAsync([], CancellationToken.None);

            await publisher.DidNotReceive().PublishAsync(Arg.Any<Null>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
        }

        [Fact]
        public async Task EnqueueBatchAsync_WithNullCollection_ShouldThrow()
        {
            var publisher = Substitute.For<IEventStreamPublisher<Null, string>>();
            var serializer = Substitute.For<ISerializer<string>>();
            var queue = new KafkaBatchSenderQueue<string>(publisher, serializer, "topic-batch");

            await Assert.ThrowsAsync<ArgumentNullException>(() =>
                queue.EnqueueBatchAsync(null, CancellationToken.None));
        }
    }
}
