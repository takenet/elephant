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
    public class KafkaSchemaRegistryPartitionSenderQueueFacts : IDisposable
    {
        private readonly string _bootstrapServers = "localhost:9092";
        private readonly string _schemaRegistryUrl = "http://localhost:8081";
        private readonly CancellationTokenSource _cts;

        public KafkaSchemaRegistryPartitionSenderQueueFacts()
        {
            _cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        }

        private SchemaRegistryOptions CreateSchemaRegistryOptions()
        {
            return new SchemaRegistryOptions(_schemaRegistryUrl, SchemaRegistrySerializerType.JSON);
        }

        [Fact]
        public async Task EnqueueWithPartitionKeyShouldSucceed()
        {
            var topic = "test-schema-registry-partition-sender-" + Guid.NewGuid().ToString("N");
            var schemaRegistryOptions = CreateSchemaRegistryOptions();

            var producerConfig = new ProducerConfig { BootstrapServers = _bootstrapServers };

            using var sender = new KafkaSchemaRegistryPartitionSenderQueue<SchemaRegistryTestItem>(
                producerConfig, topic, schemaRegistryOptions);

            var partitionKey = "partition-key-1";
            var item = new SchemaRegistryTestItem
            {
                Id = Guid.NewGuid().ToString(),
                Name = "Partition Test",
                Value = 111,
                CreatedAt = DateTime.UtcNow
            };

            await sender.EnqueueAsync(item, partitionKey, _cts.Token);
        }

        [Fact]
        public async Task EnqueueMultipleItemsWithSamePartitionKeyShouldSucceed()
        {
            var topic = "test-schema-registry-partition-sender-same-key-" + Guid.NewGuid().ToString("N");
            var schemaRegistryOptions = CreateSchemaRegistryOptions();

            var producerConfig = new ProducerConfig { BootstrapServers = _bootstrapServers };

            using var sender = new KafkaSchemaRegistryPartitionSenderQueue<SchemaRegistryTestItem>(
                producerConfig, topic, schemaRegistryOptions);

            var partitionKey = "same-partition-key";
            var items = Enumerable.Range(1, 5).Select(i => new SchemaRegistryTestItem
            {
                Id = Guid.NewGuid().ToString(),
                Name = $"Same Partition Item {i}",
                Value = i,
                CreatedAt = DateTime.UtcNow
            }).ToList();

            foreach (var item in items)
            {
                await sender.EnqueueAsync(item, partitionKey, _cts.Token);
            }
        }

        [Fact]
        public async Task EnqueueItemsWithDifferentPartitionKeysShouldSucceed()
        {
            var topic = "test-schema-registry-partition-sender-diff-keys-" + Guid.NewGuid().ToString("N");
            var schemaRegistryOptions = CreateSchemaRegistryOptions();

            var producerConfig = new ProducerConfig { BootstrapServers = _bootstrapServers };

            using var sender = new KafkaSchemaRegistryPartitionSenderQueue<SchemaRegistryTestItem>(
                producerConfig, topic, schemaRegistryOptions);

            var items = Enumerable.Range(1, 5).Select(i => new
            {
                PartitionKey = $"partition-{i}",
                Item = new SchemaRegistryTestItem
                {
                    Id = Guid.NewGuid().ToString(),
                    Name = $"Different Partition Item {i}",
                    Value = i * 10,
                    CreatedAt = DateTime.UtcNow
                }
            }).ToList();

            foreach (var entry in items)
            {
                await sender.EnqueueAsync(entry.Item, entry.PartitionKey, _cts.Token);
            }
        }

        [Fact]
        public async Task SenderWithBootstrapServerConstructorShouldSucceed()
        {
            var topic = "test-schema-registry-partition-bootstrap-" + Guid.NewGuid().ToString("N");
            var schemaRegistryOptions = CreateSchemaRegistryOptions();

            using var sender = new KafkaSchemaRegistryPartitionSenderQueue<SchemaRegistryTestItem>(
                _bootstrapServers, topic, schemaRegistryOptions);

            var partitionKey = "bootstrap-partition-key";
            var item = new SchemaRegistryTestItem
            {
                Id = Guid.NewGuid().ToString(),
                Name = "Bootstrap Partition Test",
                Value = 222,
                CreatedAt = DateTime.UtcNow
            };

            await sender.EnqueueAsync(item, partitionKey, _cts.Token);
        }

        [Fact]
        public async Task SenderWithSharedSchemaRegistryClientShouldSucceed()
        {
            var topic = "test-schema-registry-partition-shared-" + Guid.NewGuid().ToString("N");
            var schemaRegistryOptions = CreateSchemaRegistryOptions();

            var producerConfig = new ProducerConfig { BootstrapServers = _bootstrapServers };

            using var schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryOptions.SchemaRegistryConfig);
            using var sender = new KafkaSchemaRegistryPartitionSenderQueue<SchemaRegistryTestItem>(
                producerConfig, topic, schemaRegistryClient, schemaRegistryOptions);

            var partitionKey = "shared-client-partition-key";
            var item = new SchemaRegistryTestItem
            {
                Id = Guid.NewGuid().ToString(),
                Name = "Shared Client Partition Test",
                Value = 333,
                CreatedAt = DateTime.UtcNow
            };

            await sender.EnqueueAsync(item, partitionKey, _cts.Token);
        }

        public void Dispose()
        {
            _cts?.Dispose();
        }
    }
}
