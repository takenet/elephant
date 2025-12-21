using Confluent.Kafka;
using Confluent.SchemaRegistry;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Kafka.SchemaRegistry;
using Xunit;

namespace Take.Elephant.Tests.Kafka.SchemaRegistry.Avro
{
    [Trait("Category", nameof(Kafka))]
    public class KafkaSchemaRegistryAvroPartitionSenderQueueFacts : IDisposable
    {
        private readonly string _bootstrapServers = "localhost:9092";
        private readonly string _schemaRegistryUrl = "http://localhost:8081";
        private readonly CancellationTokenSource _cts;

        public KafkaSchemaRegistryAvroPartitionSenderQueueFacts()
        {
            _cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        }

        private SchemaRegistryOptions CreateSchemaRegistryOptions()
        {
            return new SchemaRegistryOptions(_schemaRegistryUrl, SchemaRegistrySerializerType.AVRO);
        }

        [Fact]
        public async Task EnqueueWithPartitionKeyShouldSucceed()
        {
            var topic = "test-avro-partition-sender-" + Guid.NewGuid().ToString("N");
            var schemaRegistryOptions = CreateSchemaRegistryOptions();

            var producerConfig = new ProducerConfig { BootstrapServers = _bootstrapServers };

            using var sender = new KafkaSchemaRegistryPartitionSenderQueue<SchemaRegistryAvroTestItem>(
                producerConfig, topic, schemaRegistryOptions);

            var partitionKey = "avro-partition-key-1";
            var item = new SchemaRegistryAvroTestItem
            {
                Id = Guid.NewGuid().ToString(),
                Name = "Avro Partition Test",
                Value = 111,
                CreatedAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            };

            await sender.EnqueueAsync(item, partitionKey, _cts.Token);
        }

        [Fact]
        public async Task EnqueueMultipleItemsWithSamePartitionKeyShouldSucceed()
        {
            var topic = "test-avro-partition-sender-same-key-" + Guid.NewGuid().ToString("N");
            var schemaRegistryOptions = CreateSchemaRegistryOptions();

            var producerConfig = new ProducerConfig { BootstrapServers = _bootstrapServers };

            using var sender = new KafkaSchemaRegistryPartitionSenderQueue<SchemaRegistryAvroTestItem>(
                producerConfig, topic, schemaRegistryOptions);

            var partitionKey = "avro-same-partition-key";
            var items = Enumerable.Range(1, 5).Select(i => new SchemaRegistryAvroTestItem
            {
                Id = Guid.NewGuid().ToString(),
                Name = $"Avro Same Partition Item {i}",
                Value = i,
                CreatedAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            }).ToList();

            foreach (var item in items)
            {
                await sender.EnqueueAsync(item, partitionKey, _cts.Token);
            }
        }

        [Fact]
        public async Task EnqueueItemsWithDifferentPartitionKeysShouldSucceed()
        {
            var topic = "test-avro-partition-sender-diff-keys-" + Guid.NewGuid().ToString("N");
            var schemaRegistryOptions = CreateSchemaRegistryOptions();

            var producerConfig = new ProducerConfig { BootstrapServers = _bootstrapServers };

            using var sender = new KafkaSchemaRegistryPartitionSenderQueue<SchemaRegistryAvroTestItem>(
                producerConfig, topic, schemaRegistryOptions);

            var items = Enumerable.Range(1, 5).Select(i => new
            {
                PartitionKey = $"avro-partition-{i}",
                Item = new SchemaRegistryAvroTestItem
                {
                    Id = Guid.NewGuid().ToString(),
                    Name = $"Avro Different Partition Item {i}",
                    Value = i * 10,
                    CreatedAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
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
            var topic = "test-avro-partition-bootstrap-" + Guid.NewGuid().ToString("N");
            var schemaRegistryOptions = CreateSchemaRegistryOptions();

            using var sender = new KafkaSchemaRegistryPartitionSenderQueue<SchemaRegistryAvroTestItem>(
                _bootstrapServers, topic, schemaRegistryOptions);

            var partitionKey = "avro-bootstrap-partition-key";
            var item = new SchemaRegistryAvroTestItem
            {
                Id = Guid.NewGuid().ToString(),
                Name = "Avro Bootstrap Partition Test",
                Value = 222,
                CreatedAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            };

            await sender.EnqueueAsync(item, partitionKey, _cts.Token);
        }

        [Fact]
        public async Task SenderWithSharedSchemaRegistryClientShouldSucceed()
        {
            var topic = "test-avro-partition-shared-" + Guid.NewGuid().ToString("N");
            var schemaRegistryOptions = CreateSchemaRegistryOptions();

            var producerConfig = new ProducerConfig { BootstrapServers = _bootstrapServers };

            using var schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryOptions.SchemaRegistryConfig);
            using var sender = new KafkaSchemaRegistryPartitionSenderQueue<SchemaRegistryAvroTestItem>(
                producerConfig, topic, schemaRegistryClient, schemaRegistryOptions);

            var partitionKey = "avro-shared-client-partition-key";
            var item = new SchemaRegistryAvroTestItem
            {
                Id = Guid.NewGuid().ToString(),
                Name = "Avro Shared Client Partition Test",
                Value = 333,
                CreatedAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            };

            await sender.EnqueueAsync(item, partitionKey, _cts.Token);
        }

        public void Dispose()
        {
            _cts?.Dispose();
        }
    }
}

