using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using NSubstitute;
using Take.Elephant.Kafka;
using Take.Elephant.Kafka.SchemaRegistry;
using Xunit;

namespace Take.Elephant.Tests.Kafka
{
    [Trait("Category", nameof(Kafka))]
    public class KafkaReceiverQueueHeadersFacts
    {
        [Fact]
        public async Task KafkaReceiverQueue_ShouldReturnPayloadAndHeaders()
        {
            // Arrange
            var expectedItem = new TestItem { Value = "payload" };
            var serializer = Substitute.For<Take.Elephant.ISerializer<TestItem>>();
            serializer.Deserialize("serialized-value").Returns(expectedItem);

            var consumer = Substitute.For<IConsumer<Ignore, string>>();
            consumer.Subscription.Returns(new List<string>());

            var consumeResult = new ConsumeResult<Ignore, string>
            {
                Message = new Message<Ignore, string>
                {
                    Value = "serialized-value",
                    Headers = CreateHeaders(
                        ("x-origin", "billing"),
                        ("x-correlation-id", "corr-123")
                    )
                }
            };

            SetupConsumeSequence(consumer, consumeResult);

            var queue = new KafkaReceiverQueue<TestItem>(consumer, serializer, "topic-a");

            // Act
            var consumed = await queue.DequeueWithHeadersAsync(CancellationToken.None);

            // Assert
            Assert.Same(expectedItem, consumed.Item);
            Assert.True(consumed.TryGetHeaderAsUtf8String("x-origin", out var origin));
            Assert.Equal("billing", origin);
            Assert.True(consumed.TryGetHeaderAsUtf8String("x-correlation-id", out var correlationId));
            Assert.Equal("corr-123", correlationId);

            await queue.CloseAsync(CancellationToken.None);
        }

        [Fact]
        public async Task KafkaSchemaRegistryReceiverQueue_ShouldReturnPayloadAndHeaders()
        {
            // Arrange
            var expectedItem = new TestItem { Value = "payload" };
            var consumer = Substitute.For<IConsumer<Ignore, TestItem>>();
            consumer.Subscription.Returns(new List<string>());

            var consumeResult = new ConsumeResult<Ignore, TestItem>
            {
                Message = new Message<Ignore, TestItem>
                {
                    Value = expectedItem,
                    Headers = CreateHeaders(
                        ("x-origin", "billing"),
                        ("x-correlation-id", "corr-123")
                    )
                }
            };

            SetupConsumeSequence(consumer, consumeResult);

            var queue = new KafkaSchemaRegistryReceiverQueue<TestItem>(consumer, "topic-a");

            // Act
            var consumed = await queue.DequeueWithHeadersAsync(CancellationToken.None);

            // Assert
            Assert.Same(expectedItem, consumed.Item);
            Assert.True(consumed.TryGetHeaderAsUtf8String("x-origin", out var origin));
            Assert.Equal("billing", origin);
            Assert.True(consumed.TryGetHeaderAsUtf8String("x-correlation-id", out var correlationId));
            Assert.Equal("corr-123", correlationId);

            await queue.CloseAsync(CancellationToken.None);
        }

        [Fact]
        public async Task KafkaReceiverQueue_DequeueAsync_ThenDequeueWithHeadersOrDefaultAsync_ShouldReturnNull_WhenQueueIsEmpty()
        {
            // Arrange
            var expectedItem = new TestItem { Value = "payload" };
            var serializer = Substitute.For<Take.Elephant.ISerializer<TestItem>>();
            serializer.Deserialize("serialized-value").Returns(expectedItem);

            var consumer = Substitute.For<IConsumer<Ignore, string>>();
            consumer.Subscription.Returns(new List<string>());

            var consumeResult = new ConsumeResult<Ignore, string>
            {
                Message = new Message<Ignore, string>
                {
                    Value = "serialized-value",
                    Headers = null,
                }
            };

            SetupConsumeSequence(consumer, consumeResult);

            var queue = new KafkaReceiverQueue<TestItem>(consumer, serializer, "topic-a");

            // Act
            var item = await queue.DequeueAsync(CancellationToken.None);
            var consumed = await queue.DequeueWithHeadersOrDefaultAsync(CancellationToken.None);

            // Assert
            Assert.Same(expectedItem, item);
            Assert.Null(consumed);

            await queue.CloseAsync(CancellationToken.None);
        }

        [Fact]
        public async Task KafkaSchemaRegistryReceiverQueue_DequeueWithHeadersAsync_ShouldReturnEmptyHeaders_WhenMessageHasNoHeaders()
        {
            // Arrange
            var expectedItem = new TestItem { Value = "payload" };
            var consumer = Substitute.For<IConsumer<Ignore, TestItem>>();
            consumer.Subscription.Returns(new List<string>());

            var consumeResult = new ConsumeResult<Ignore, TestItem>
            {
                Message = new Message<Ignore, TestItem>
                {
                    Value = expectedItem,
                    Headers = null,
                }
            };

            SetupConsumeSequence(consumer, consumeResult);

            var queue = new KafkaSchemaRegistryReceiverQueue<TestItem>(consumer, "topic-a");

            // Act
            var consumed = await queue.DequeueWithHeadersAsync(CancellationToken.None);

            // Assert
            Assert.Same(expectedItem, consumed.Item);
            Assert.Empty(consumed.Headers);
            Assert.False(consumed.TryGetHeaderAsUtf8String("x-origin", out _));

            await queue.CloseAsync(CancellationToken.None);
        }

        private static void SetupConsumeSequence<TKey, TValue>(
            IConsumer<TKey, TValue> consumer,
            ConsumeResult<TKey, TValue> firstResult)
        {
            var calls = 0;
            consumer
                .Consume(Arg.Any<CancellationToken>())
                .Returns(callInfo =>
                {
                    var cancellationToken = callInfo.Arg<CancellationToken>();
                    calls++;
                    if (calls == 1)
                    {
                        return firstResult;
                    }

                    cancellationToken.WaitHandle.WaitOne();
                    throw new OperationCanceledException(cancellationToken);
                });
        }

        private static Headers CreateHeaders(params (string Key, string Value)[] headers)
        {
            var result = new Headers();
            foreach (var (key, value) in headers)
            {
                result.Add(key, Encoding.UTF8.GetBytes(value));
            }

            return result;
        }

        public sealed class TestItem
        {
            public string Value { get; set; }
        }
    }
}