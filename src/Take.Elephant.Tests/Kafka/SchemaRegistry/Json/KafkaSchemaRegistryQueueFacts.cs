using Confluent.Kafka;
using Confluent.SchemaRegistry;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Kafka.SchemaRegistry;
using Xunit;

namespace Take.Elephant.Tests.Kafka.SchemaRegistry.Json
{
    [Trait("Category", nameof(Kafka))]
    public class KafkaSchemaRegistryQueueFacts : IDisposable
    {
        private readonly string _bootstrapServers = "localhost:9092";
        private readonly string _schemaRegistryUrl = "http://localhost:8081";
        private readonly CancellationTokenSource _cts;

        public KafkaSchemaRegistryQueueFacts()
        {
            _cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        }

        private SchemaRegistryOptions CreateSchemaRegistryOptions()
        {
            return new SchemaRegistryOptions(_schemaRegistryUrl, SchemaRegistrySerializerType.JSON);
        }

        [Fact]
        public async Task EnqueueAndDequeueSingleItemShouldSucceed()
        {
            var topic = "test-schema-registry-queue-" + Guid.NewGuid().ToString("N");
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

            using var queue = new KafkaSchemaRegistryQueue<SchemaRegistryTestItem>(
                producerConfig, consumerConfig, topic, schemaRegistryOptions);

            var item = new SchemaRegistryTestItem
            {
                Id = Guid.NewGuid().ToString(),
                Name = "Queue Test",
                Value = 500,
                CreatedAt = DateTime.UtcNow
            };

            await queue.EnqueueAsync(item, _cts.Token);

            var result = await queue.DequeueAsync(_cts.Token);

            Assert.NotNull(result);
            Assert.Equal(item.Id, result.Id);
            Assert.Equal(item.Name, result.Name);
            Assert.Equal(item.Value, result.Value);

            await queue.CloseAsync(_cts.Token);
        }

        [Fact]
        public async Task EnqueueAndDequeueMultipleItemsShouldSucceed()
        {
            var topic = "test-schema-registry-queue-multi-" + Guid.NewGuid().ToString("N");
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

            using var queue = new KafkaSchemaRegistryQueue<SchemaRegistryTestItem>(
                producerConfig, consumerConfig, topic, schemaRegistryOptions);

            var items = Enumerable.Range(1, 5).Select(i => new SchemaRegistryTestItem
            {
                Id = Guid.NewGuid().ToString(),
                Name = $"Queue Item {i}",
                Value = i * 100,
                CreatedAt = DateTime.UtcNow
            }).ToList();

            foreach (var item in items)
            {
                await queue.EnqueueAsync(item, _cts.Token);
            }

            var receivedItems = new List<SchemaRegistryTestItem>();
            for (int i = 0; i < items.Count; i++)
            {
                var result = await queue.DequeueAsync(_cts.Token);
                if (result != null)
                {
                    receivedItems.Add(result);
                }
            }

            Assert.Equal(items.Count, receivedItems.Count);

            await queue.CloseAsync(_cts.Token);
        }

        [Fact]
        public async Task QueueWithBootstrapServerConstructorShouldSucceed()
        {
            var topic = "test-schema-registry-queue-bootstrap-" + Guid.NewGuid().ToString("N");
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

            using var queue = new KafkaSchemaRegistryQueue<SchemaRegistryTestItem>(
                producerConfig, consumerConfig, topic, schemaRegistryOptions);

            var item = new SchemaRegistryTestItem
            {
                Id = Guid.NewGuid().ToString(),
                Name = "Bootstrap Queue Test",
                Value = 321,
                CreatedAt = DateTime.UtcNow
            };

            await queue.EnqueueAsync(item, _cts.Token);

            var result = await queue.DequeueAsync(_cts.Token);

            Assert.NotNull(result);
            Assert.Equal(item.Id, result.Id);

            await queue.CloseAsync(_cts.Token);
        }

        [Fact]
        public async Task QueueWithSharedSchemaRegistryClientShouldSucceed()
        {
            var topic = "test-schema-registry-queue-shared-" + Guid.NewGuid().ToString("N");
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
            using var queue = new KafkaSchemaRegistryQueue<SchemaRegistryTestItem>(
                producerConfig, consumerConfig, topic, schemaRegistryClient, schemaRegistryOptions);

            var item = new SchemaRegistryTestItem
            {
                Id = Guid.NewGuid().ToString(),
                Name = "Shared Client Queue Test",
                Value = 654,
                CreatedAt = DateTime.UtcNow
            };

            await queue.EnqueueAsync(item, _cts.Token);

            var result = await queue.DequeueAsync(_cts.Token);

            Assert.NotNull(result);
            Assert.Equal(item.Id, result.Id);

            await queue.CloseAsync(_cts.Token);
        }

        [Fact]
        public async Task DequeueOrDefaultOnEmptyQueueShouldReturnNull()
        {
            var topic = "test-schema-registry-queue-empty-" + Guid.NewGuid().ToString("N");
            var groupId = Guid.NewGuid().ToString();
            var schemaRegistryOptions = CreateSchemaRegistryOptions();

            using var queue = new KafkaSchemaRegistryQueue<SchemaRegistryTestItem>(
                _bootstrapServers, topic, groupId, schemaRegistryOptions);

            var result = await queue.DequeueOrDefaultAsync(_cts.Token);

            Assert.Null(result);

            await queue.CloseAsync(_cts.Token);
        }

        public void Dispose()
        {
            _cts?.Dispose();
        }
    }
}
