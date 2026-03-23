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
    public class KafkaSenderAndSchemaRegistryCoverageFacts
    {
        [Fact]
        public void KafkaSenderQueue_BootstrapCtor_ShouldCreateInstance()
        {
            var serializer = Substitute.For<Take.Elephant.ISerializer<TestItem>>();

            var queue = new KafkaSenderQueue<TestItem>("localhost:9092", "topic-a", serializer);

            Assert.Equal("topic-a", queue.Topic);
            queue.Dispose();
        }

        [Fact]
        public void KafkaSenderQueue_BootstrapCtorWithKafkaSerializer_ShouldCreateInstance()
        {
            var serializer = Substitute.For<Take.Elephant.ISerializer<TestItem>>();
            var kafkaSerializer = Substitute.For<Confluent.Kafka.ISerializer<string>>();

            var queue = new KafkaSenderQueue<TestItem>(
                "localhost:9092",
                "topic-a",
                serializer,
                kafkaSerializer
            );

            Assert.Equal("topic-a", queue.Topic);
            queue.Dispose();
        }

        [Fact]
        public void KafkaSenderQueue_WithIProducerCtor_ShouldCreateInstance()
        {
            var producer = Substitute.For<IProducer<Null, string>>();
            var serializer = Substitute.For<Take.Elephant.ISerializer<TestItem>>();

            var queue = new KafkaSenderQueue<TestItem>(producer, serializer, "topic-a");

            Assert.Equal("topic-a", queue.Topic);
            queue.Dispose();
        }

        [Fact]
        public void KafkaSchemaRegistrySenderQueue_WithIProducerCtor_ShouldThrowForWhitespaceTopic()
        {
            var producer = Substitute.For<IProducer<Null, TestItem>>();

            Assert.Throws<ArgumentException>(
                () => new KafkaSchemaRegistrySenderQueue<TestItem>(producer, " ")
            );
        }

        [Fact]
        public async Task KafkaSchemaRegistrySenderQueue_WithHeaderProvider_ShouldInjectHeadersOnEnqueue()
        {
            var producer = Substitute.For<IProducer<Null, TestItem>>();
            var headerProvider = Substitute.For<IKafkaHeaderProvider>();
            headerProvider
                .GetHeaders()
                .Returns(
                    new List<IHeader>
                    {
                        new Header("x-origin", Encoding.UTF8.GetBytes("billing"))
                    }
                );

            Message<Null, TestItem> capturedMessage = null;
            producer
                .ProduceAsync(
                    Arg.Any<string>(),
                    Arg.Do<Message<Null, TestItem>>(m => capturedMessage = m),
                    Arg.Any<CancellationToken>()
                )
                .Returns(Task.FromResult(new DeliveryResult<Null, TestItem>()));

            var queue = new KafkaSchemaRegistrySenderQueue<TestItem>(
                producer,
                "topic-a",
                schemaRegistryClient: null,
                headerProvider: headerProvider
            );

            await queue.EnqueueAsync(new TestItem(), CancellationToken.None);

            Assert.NotNull(capturedMessage);
            Assert.NotNull(capturedMessage.Headers);
            Assert.Single(capturedMessage.Headers);
            Assert.Equal("x-origin", capturedMessage.Headers[0].Key);
            Assert.Equal("billing", Encoding.UTF8.GetString(capturedMessage.Headers[0].GetValueBytes()));

            queue.Dispose();
        }

        [Fact]
        public void KafkaEventStreamPublisher_WithIProducerCtor_ShouldThrowForNullProducer()
        {
            Assert.Throws<ArgumentNullException>(
                () => new KafkaEventStreamPublisher<string, TestItem>(null, "topic-a")
            );
        }

        [Fact]
        public void KafkaEventStreamPublisher_WithSerializerCtor_ShouldThrowForNullSerializer()
        {
            var producerConfig = new ProducerConfig { BootstrapServers = "localhost:9092" };

            var exception = Assert.Throws<ArgumentNullException>(
                () => new KafkaEventStreamPublisher<string, TestItem>(producerConfig, "topic-a", (Take.Elephant.ISerializer<TestItem>)null)
            );

            Assert.Equal("serializer", exception.ParamName);
        }

        [Fact]
        public void KafkaEventStreamPublisher_WithSerializerCtor_ShouldThrowForNullProducerConfig()
        {
            var serializer = Substitute.For<Take.Elephant.ISerializer<TestItem>>();

            var exception = Assert.Throws<ArgumentNullException>(
                () => new KafkaEventStreamPublisher<string, TestItem>((ProducerConfig)null, "topic-a", serializer)
            );

            Assert.Equal("producerConfig", exception.ParamName);
        }

        [Fact]
        public void KafkaEventStreamPublisher_WithKafkaSerializerCtor_ShouldThrowForNullKafkaSerializer()
        {
            var producerConfig = new ProducerConfig { BootstrapServers = "localhost:9092" };

            var exception = Assert.Throws<ArgumentNullException>(
                () => new KafkaEventStreamPublisher<string, TestItem>(producerConfig, "topic-a", (Confluent.Kafka.ISerializer<TestItem>)null)
            );

            Assert.Equal("kafkaSerializer", exception.ParamName);
        }

        [Fact]
        public async Task KafkaEventStreamPublisher_WithHeaderProviderReturningNull_ShouldKeepEmptyHeaders()
        {
            var producer = Substitute.For<IProducer<string, TestItem>>();
            var headerProvider = Substitute.For<IKafkaHeaderProvider>();
            headerProvider.GetHeaders().Returns((IEnumerable<IHeader>)null);

            Message<string, TestItem> capturedMessage = null;
            producer
                .ProduceAsync(
                    Arg.Any<string>(),
                    Arg.Do<Message<string, TestItem>>(m => capturedMessage = m),
                    Arg.Any<CancellationToken>()
                )
                .Returns(Task.FromResult(new DeliveryResult<string, TestItem>()));

            var publisher = new KafkaEventStreamPublisher<string, TestItem>(
                producer,
                "topic-a",
                headerProvider
            );

            await publisher.PublishAsync("key-1", new TestItem(), CancellationToken.None);

            Assert.NotNull(capturedMessage);
            Assert.Null(capturedMessage.Headers);

            publisher.Dispose();
        }

        [Fact]
        public async Task KafkaEventStreamPublisher_WithInvalidHeaders_ShouldIgnoreInvalidAndKeepValid()
        {
            var producer = Substitute.For<IProducer<string, TestItem>>();
            var headerProvider = Substitute.For<IKafkaHeaderProvider>();

            var nullKeyHeader = Substitute.For<IHeader>();
            nullKeyHeader.Key.Returns((string)null);
            nullKeyHeader.GetValueBytes().Returns(Encoding.UTF8.GetBytes("ignored"));

            headerProvider
                .GetHeaders()
                .Returns(
                    new List<IHeader>
                    {
                        null,
                        nullKeyHeader,
                        new Header("x-origin", Encoding.UTF8.GetBytes("billing")),
                    }
                );

            Message<string, TestItem> capturedMessage = null;
            producer
                .ProduceAsync(
                    Arg.Any<string>(),
                    Arg.Do<Message<string, TestItem>>(m => capturedMessage = m),
                    Arg.Any<CancellationToken>()
                )
                .Returns(Task.FromResult(new DeliveryResult<string, TestItem>()));

            var publisher = new KafkaEventStreamPublisher<string, TestItem>(
                producer,
                "topic-a",
                headerProvider
            );

            await publisher.PublishAsync("key-1", new TestItem(), CancellationToken.None);

            Assert.NotNull(capturedMessage);
            Assert.NotNull(capturedMessage.Headers);
            Assert.Single(capturedMessage.Headers);
            Assert.Equal("x-origin", capturedMessage.Headers[0].Key);
            Assert.Equal("billing", Encoding.UTF8.GetString(capturedMessage.Headers[0].GetValueBytes()));

            publisher.Dispose();
        }

        public sealed class TestItem
        {
            public string Value { get; set; }
        }
    }
}
