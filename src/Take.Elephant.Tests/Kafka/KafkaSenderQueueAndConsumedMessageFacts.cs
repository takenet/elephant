using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
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
    public class KafkaSenderQueueAndConsumedMessageFacts
    {
        [Fact]
        public async Task KafkaSenderQueue_WithEventStreamPublisherCtor_ShouldSetPropertiesAndEnqueue()
        {
            var publisher = Substitute.For<IEventStreamPublisher<Null, string>>();
            var serializer = Substitute.For<Take.Elephant.ISerializer<TestItem>>();
            serializer.Serialize(Arg.Any<TestItem>()).Returns("serialized");

            var queue = new KafkaSenderQueue<TestItem>(publisher, serializer, "topic-a");

            Assert.Equal("topic-a", queue.Topic);
            await queue.EnqueueAsync(new TestItem(), CancellationToken.None);

            await publisher.Received(1).PublishAsync(null, "serialized", CancellationToken.None);
        }

        [Fact]
        public void KafkaSenderQueue_WithEventStreamPublisherCtor_ShouldThrowWhenProducerIsNull()
        {
            var serializer = Substitute.For<Take.Elephant.ISerializer<TestItem>>();

            var ex = Assert.Throws<ArgumentNullException>(
                () => new KafkaSenderQueue<TestItem>((IEventStreamPublisher<Null, string>)null, serializer, "topic-a")
            );

            Assert.Equal("producer", ex.ParamName);
        }

        [Fact]
        public void KafkaSenderQueue_WithEventStreamPublisherCtor_ShouldThrowWhenSerializerIsNull()
        {
            var publisher = Substitute.For<IEventStreamPublisher<Null, string>>();

            var ex = Assert.Throws<ArgumentNullException>(
                () => new KafkaSenderQueue<TestItem>(publisher, null, "topic-a")
            );

            Assert.Equal("serializer", ex.ParamName);
        }

        [Fact]
        public void KafkaSenderQueue_WithEventStreamPublisherCtor_ShouldThrowWhenTopicIsNull()
        {
            var publisher = Substitute.For<IEventStreamPublisher<Null, string>>();
            var serializer = Substitute.For<Take.Elephant.ISerializer<TestItem>>();

            var ex = Assert.Throws<ArgumentNullException>(
                () => new KafkaSenderQueue<TestItem>(publisher, serializer, null)
            );

            Assert.Equal("topic", ex.ParamName);
        }

        [Fact]
        public void KafkaConsumedMessage_WithNullHeaders_ShouldExposeEmptyReadOnlyHeaders()
        {
            var consumed = new KafkaConsumedMessage<TestItem>(new TestItem(), null);

            Assert.Empty(consumed.Headers);
            Assert.False(consumed.TryGetHeader("x-missing", out _));
            Assert.False(consumed.TryGetHeaderAsUtf8String("x-missing", out _));
        }

        [Fact]
        public void KafkaConsumedMessage_TryGetHeader_ShouldReturnTrueWhenHeaderExists()
        {
            var expectedBytes = Encoding.UTF8.GetBytes("value-1");
            var headers = new Dictionary<string, byte[]>
            {
                ["x-key"] = expectedBytes
            };

            var consumed = new KafkaConsumedMessage<TestItem>(new TestItem(), headers);

            Assert.True(consumed.TryGetHeader("x-key", out var actual));
            Assert.Equal(expectedBytes, actual);
        }

        [Fact]
        public void KafkaConsumedMessage_ShouldCloneHeaderValuesOnConstruction()
        {
            var sourceBytes = new byte[] { 1, 2, 3 };
            var headers = new Dictionary<string, byte[]>
            {
                ["x-key"] = sourceBytes
            };

            var consumed = new KafkaConsumedMessage<TestItem>(new TestItem(), headers);
            sourceBytes[0] = 9;

            Assert.True(consumed.TryGetHeader("x-key", out var actual));
            Assert.Equal(new byte[] { 1, 2, 3 }, actual);
        }

        [Fact]
        public void KafkaConsumedMessage_TryGetHeader_ShouldReturnClonedValue()
        {
            var headers = new Dictionary<string, byte[]>
            {
                ["x-key"] = new byte[] { 1, 2, 3 }
            };
            var consumed = new KafkaConsumedMessage<TestItem>(new TestItem(), headers);

            Assert.True(consumed.TryGetHeader("x-key", out var firstRead));
            firstRead[0] = 9;

            Assert.True(consumed.TryGetHeader("x-key", out var secondRead));
            Assert.Equal(new byte[] { 1, 2, 3 }, secondRead);
        }

        [Fact]
        public void KafkaConsumedMessage_WithTrustedHeaders_ShouldReuseProvidedValues()
        {
            var headerValue = new byte[] { 1, 2, 3 };
            IReadOnlyDictionary<string, byte[]> headers =
                new ReadOnlyDictionary<string, byte[]>(new Dictionary<string, byte[]>
                {
                    ["x-key"] = headerValue
                });

            var consumed = new KafkaConsumedMessage<TestItem>(new TestItem(), headers, headersAreSafe: true);

            Assert.Same(headerValue, consumed.Headers["x-key"]);
            Assert.True(consumed.TryGetHeader("x-key", out var headerCopy));
            Assert.NotSame(headerValue, headerCopy);
            Assert.Equal(headerValue, headerCopy);
        }

        [Fact]
        public void KafkaConsumedMessage_TryGetHeader_ShouldReturnFalseForNullOrWhitespaceKey()
        {
            var consumed = new KafkaConsumedMessage<TestItem>(new TestItem(), new Dictionary<string, byte[]>());

            Assert.False(consumed.TryGetHeader(null, out _));
            Assert.False(consumed.TryGetHeader("", out _));
            Assert.False(consumed.TryGetHeader("   ", out _));
        }

        public sealed class TestItem
        {
            public string Value { get; set; }
        }
    }
}
