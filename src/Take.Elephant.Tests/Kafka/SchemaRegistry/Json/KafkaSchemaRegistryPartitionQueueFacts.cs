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
    public class KafkaSchemaRegistryPartitionQueueFacts : IDisposable
    {
        private readonly string _bootstrapServers = "localhost:9092";
        private readonly string _schemaRegistryUrl = "http://localhost:8081";
        private readonly CancellationTokenSource _cts;

        public KafkaSchemaRegistryPartitionQueueFacts()
        {
            _cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        }

        private SchemaRegistryOptions CreateSchemaRegistryOptions()
        {
            return new SchemaRegistryOptions(_schemaRegistryUrl, SchemaRegistrySerializerType.JSON);
        }

        [Fact]
        public async Task EnqueueAndDequeueWithPartitionKeyShouldSucceed()
        {
            var topic = "test-schema-registry-partition-queue-" + Guid.NewGuid().ToString("N");
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

            using var senderQueue = new KafkaSchemaRegistryPartitionSenderQueue<SchemaRegistryTestItem>(
                producerConfig, topic, schemaRegistryOptions);
            using var receiverQueue = new KafkaSchemaRegistryReceiverQueue<SchemaRegistryTestItem>(
                consumerConfig, topic, schemaRegistryOptions);

            var partitionKey = "partition-key-1";
            var item = new SchemaRegistryTestItem
            {
                Id = Guid.NewGuid().ToString(),
                Name = "Partition Queue Test",
                Value = 444,
                CreatedAt = DateTime.UtcNow
            };

            await senderQueue.EnqueueAsync(item, partitionKey, _cts.Token);

            await receiverQueue.OpenAsync(_cts.Token);

            var result = await receiverQueue.DequeueAsync(_cts.Token);

            Assert.NotNull(result);
            Assert.Equal(item.Id, result.Id);
            Assert.Equal(item.Name, result.Name);
            Assert.Equal(item.Value, result.Value);

            await receiverQueue.CloseAsync(_cts.Token);
        }

        [Fact]
        public async Task EnqueueMultipleItemsWithPartitionKeysShouldSucceed()
        {
            var topic = "test-schema-registry-partition-queue-multi-" + Guid.NewGuid().ToString("N");
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

            using var senderQueue = new KafkaSchemaRegistryPartitionSenderQueue<SchemaRegistryTestItem>(
                producerConfig, topic, schemaRegistryOptions);
            using var receiverQueue = new KafkaSchemaRegistryReceiverQueue<SchemaRegistryTestItem>(
                consumerConfig, topic, schemaRegistryOptions);

            var items = Enumerable.Range(1, 5).Select(i => new
            {
                PartitionKey = $"partition-{i}",
                Item = new SchemaRegistryTestItem
                {
                    Id = Guid.NewGuid().ToString(),
                    Name = $"Partition Queue Item {i}",
                    Value = i * 100,
                    CreatedAt = DateTime.UtcNow
                }
            }).ToList();

            foreach (var entry in items)
            {
                await senderQueue.EnqueueAsync(entry.Item, entry.PartitionKey, _cts.Token);
            }

            await receiverQueue.OpenAsync(_cts.Token);

            var receivedItems = new List<SchemaRegistryTestItem>();
            for (int i = 0; i < items.Count; i++)
            {
                var result = await receiverQueue.DequeueAsync(_cts.Token);
                if (result != null)
                {
                    receivedItems.Add(result);
                }
            }

            Assert.Equal(items.Count, receivedItems.Count);

            await receiverQueue.CloseAsync(_cts.Token);
        }

        [Fact]
        public async Task PartitionQueueWithSharedSchemaRegistryClientShouldSucceed()
        {
            var topic = "test-schema-registry-partition-queue-shared-" + Guid.NewGuid().ToString("N");
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
            using var senderQueue = new KafkaSchemaRegistryPartitionSenderQueue<SchemaRegistryTestItem>(
                producerConfig, topic, schemaRegistryClient, schemaRegistryOptions);
            using var receiverQueue = new KafkaSchemaRegistryReceiverQueue<SchemaRegistryTestItem>(
                consumerConfig, topic, schemaRegistryClient, schemaRegistryOptions);

            var partitionKey = "shared-partition-key";
            var item = new SchemaRegistryTestItem
            {
                Id = Guid.NewGuid().ToString(),
                Name = "Shared Client Partition Queue Test",
                Value = 666,
                CreatedAt = DateTime.UtcNow
            };

            await senderQueue.EnqueueAsync(item, partitionKey, _cts.Token);

            await receiverQueue.OpenAsync(_cts.Token);

            var result = await receiverQueue.DequeueAsync(_cts.Token);

            Assert.NotNull(result);
            Assert.Equal(item.Id, result.Id);

            await receiverQueue.CloseAsync(_cts.Token);
        }

        public void Dispose()
        {
            _cts?.Dispose();
        }
    }
}