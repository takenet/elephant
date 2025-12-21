using Confluent.Kafka;
using Confluent.SchemaRegistry;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Kafka.SchemaRegistry;
using Xunit;

namespace Take.Elephant.Tests.Kafka.SchemaRegistry.Json
{
    [Trait("Category", nameof(Kafka))]
    public class KafkaSchemaRegistryEventStreamPublisherConsumerFacts : IDisposable
    {
        private readonly string _bootstrapServers = "localhost:9092";
        private readonly string _schemaRegistryUrl = "http://localhost:8081";
        private readonly CancellationTokenSource _cts;

        public KafkaSchemaRegistryEventStreamPublisherConsumerFacts()
        {
            _cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        }

        private SchemaRegistryOptions CreateSchemaRegistryOptions()
        {
            return new SchemaRegistryOptions(_schemaRegistryUrl, SchemaRegistrySerializerType.JSON);
        }

        [Fact]
        public async Task PublishAndConsumeWithSchemaRegistryShouldSucceed()
        {
            var topic = "test-schema-registry-stream-" + Guid.NewGuid().ToString("N");
            var groupId = Guid.NewGuid().ToString();
            var schemaRegistryOptions = CreateSchemaRegistryOptions();

            var producerConfig = new ProducerConfig { BootstrapServers = _bootstrapServers };
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = _bootstrapServers,
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };

            using var publisher = new KafkaSchemaRegistryEventStreamPublisher<string, SchemaRegistryTestItem>(
                producerConfig, topic, schemaRegistryOptions);
            using var consumer = new KafkaSchemaRegistryEventStreamConsumer<string, SchemaRegistryTestItem>(
                consumerConfig, topic, schemaRegistryOptions);

            await consumer.OpenAsync(_cts.Token);

            var key = Guid.NewGuid().ToString();
            var item = new SchemaRegistryTestItem
            {
                Id = Guid.NewGuid().ToString(),
                Name = "Test Item",
                Value = 42,
                CreatedAt = DateTime.UtcNow
            };

            await publisher.PublishAsync(key, item, _cts.Token);
            await Task.Delay(4000);

            var result = await consumer.ConsumeOrDefaultAsync(_cts.Token);

            Assert.NotNull(result.item);
            Assert.Equal(key, result.key);
            Assert.Equal(item.Id, result.item.Id);
            Assert.Equal(item.Name, result.item.Name);
            Assert.Equal(item.Value, result.item.Value);

            await consumer.CloseAsync(_cts.Token);
        }

        [Fact]
        public async Task PublishMultipleItemsAndConsumeWithSchemaRegistryShouldSucceed()
        {
            var topic = "test-schema-registry-stream-multi-" + Guid.NewGuid().ToString("N");
            var groupId = Guid.NewGuid().ToString();
            var schemaRegistryOptions = CreateSchemaRegistryOptions();

            var producerConfig = new ProducerConfig { BootstrapServers = _bootstrapServers };
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = _bootstrapServers,
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };

            using var publisher = new KafkaSchemaRegistryEventStreamPublisher<string, SchemaRegistryTestItem>(
                producerConfig, topic, schemaRegistryOptions);
            using var consumer = new KafkaSchemaRegistryEventStreamConsumer<string, SchemaRegistryTestItem>(
                consumerConfig, topic, schemaRegistryOptions);

            await consumer.OpenAsync(_cts.Token);

            var items = Enumerable.Range(1, 2).Select(i => new SchemaRegistryTestItem
            {
                Id = Guid.NewGuid().ToString(),
                Name = $"Test Item {i}",
                Value = i * 10,
                CreatedAt = DateTime.UtcNow
            }).ToList();

            foreach (var item in items)
            {
                await publisher.PublishAsync(item.Id, item, _cts.Token);
            }

            await Task.Delay(4000);

            var consumedItems = new System.Collections.Generic.List<SchemaRegistryTestItem>();
            for (int i = 0; i < items.Count; i++)
            {
                var result = await consumer.ConsumeOrDefaultAsync(_cts.Token);
                if (result.item != null)
                {
                    consumedItems.Add(result.item);
                }
            }

            Assert.Equal(items.Count, consumedItems.Count);

            await consumer.CloseAsync(_cts.Token);
        }

        [Fact]
        public async Task PublishWithSharedSchemaRegistryClientShouldSucceed()
        {
            var topic = "test-schema-registry-stream-shared-" + Guid.NewGuid().ToString("N");
            var groupId = Guid.NewGuid().ToString();
            var schemaRegistryOptions = CreateSchemaRegistryOptions();

            var producerConfig = new ProducerConfig { BootstrapServers = _bootstrapServers };
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = _bootstrapServers,
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };

            using var schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryOptions.SchemaRegistryConfig);
            using var publisher = new KafkaSchemaRegistryEventStreamPublisher<string, SchemaRegistryTestItem>(
                producerConfig, topic, schemaRegistryClient, schemaRegistryOptions);
            using var consumer = new KafkaSchemaRegistryEventStreamConsumer<string, SchemaRegistryTestItem>(
                consumerConfig, topic, schemaRegistryClient, schemaRegistryOptions);

            await consumer.OpenAsync(_cts.Token);

            var key = Guid.NewGuid().ToString();
            var item = new SchemaRegistryTestItem
            {
                Id = Guid.NewGuid().ToString(),
                Name = "Shared Client Test",
                Value = 100,
                CreatedAt = DateTime.UtcNow
            };

            await publisher.PublishAsync(key, item, _cts.Token);
            await Task.Delay(4000);

            var result = await consumer.ConsumeOrDefaultAsync(_cts.Token);

            Assert.NotNull(result.item);
            Assert.Equal(item.Id, result.item.Id);

            await consumer.CloseAsync(_cts.Token);
        }

        public void Dispose()
        {
            _cts?.Dispose();
        }
    }
}
