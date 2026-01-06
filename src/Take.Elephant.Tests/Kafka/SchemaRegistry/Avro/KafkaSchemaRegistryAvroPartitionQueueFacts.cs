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
    public class KafkaSchemaRegistryAvroPartitionQueueFacts : IDisposable
    {
        private readonly string _bootstrapServers = "localhost:9092";
        private readonly string _schemaRegistryUrl = "http://localhost:8081";
        private readonly CancellationTokenSource _cts;

        public KafkaSchemaRegistryAvroPartitionQueueFacts()
        {
            _cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        }

        private SchemaRegistryOptions CreateSchemaRegistryOptions()
        {
            return new SchemaRegistryOptions(_schemaRegistryUrl, SchemaRegistrySerializerType.AVRO);
        }

        [Fact]
        public async Task EnqueueAndDequeueWithPartitionKeyShouldSucceed()
        {
            var topic = "test-avro-partition-queue-" + Guid.NewGuid().ToString("N");
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

            using var senderQueue = new KafkaSchemaRegistryPartitionSenderQueue<SchemaRegistryAvroTestItem>(
                producerConfig, topic, schemaRegistryOptions);
            using var receiverQueue = new KafkaSchemaRegistryReceiverQueue<SchemaRegistryAvroTestItem>(
                consumerConfig, topic, schemaRegistryOptions);

            var partitionKey = "avro-partition-key-1";
            var item = new SchemaRegistryAvroTestItem
            {
                Id = Guid.NewGuid().ToString(),
                Name = "Avro Partition Queue Test",
                Value = 444,
                CreatedAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
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
            var topic = "test-avro-partition-queue-multi-" + Guid.NewGuid().ToString("N");
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

            using var senderQueue = new KafkaSchemaRegistryPartitionSenderQueue<SchemaRegistryAvroTestItem>(
                producerConfig, topic, schemaRegistryOptions);
            using var receiverQueue = new KafkaSchemaRegistryReceiverQueue<SchemaRegistryAvroTestItem>(
                consumerConfig, topic, schemaRegistryOptions);

            var items = Enumerable.Range(1, 5).Select(i => new
            {
                PartitionKey = $"avro-partition-{i}",
                Item = new SchemaRegistryAvroTestItem
                {
                    Id = Guid.NewGuid().ToString(),
                    Name = $"Avro Partition Queue Item {i}",
                    Value = i * 100,
                    CreatedAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                }
            }).ToList();

            foreach (var entry in items)
            {
                await senderQueue.EnqueueAsync(entry.Item, entry.PartitionKey, _cts.Token);
            }

            await receiverQueue.OpenAsync(_cts.Token);

            var receivedItems = new List<SchemaRegistryAvroTestItem>();
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
            var topic = "test-avro-partition-queue-shared-" + Guid.NewGuid().ToString("N");
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
            using var senderQueue = new KafkaSchemaRegistryPartitionSenderQueue<SchemaRegistryAvroTestItem>(
                producerConfig, topic, schemaRegistryClient, schemaRegistryOptions);
            using var receiverQueue = new KafkaSchemaRegistryReceiverQueue<SchemaRegistryAvroTestItem>(
                consumerConfig, topic, schemaRegistryClient, schemaRegistryOptions);

            var partitionKey = "avro-shared-partition-key";
            var item = new SchemaRegistryAvroTestItem
            {
                Id = Guid.NewGuid().ToString(),
                Name = "Avro Shared Client Partition Queue Test",
                Value = 666,
                CreatedAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            };

            await senderQueue.EnqueueAsync(item, partitionKey, _cts.Token);

            await receiverQueue.OpenAsync(_cts.Token);

            var result = await receiverQueue.DequeueAsync(_cts.Token);

            Assert.NotNull(result);
            Assert.Equal(item.Id, result.Id);

            await receiverQueue.CloseAsync(_cts.Token);
        }

        [Fact]
        public async Task PartitionQueueWithBootstrapServerConstructorShouldSucceed()
        {
            var topic = "test-avro-partition-queue-bootstrap-" + Guid.NewGuid().ToString("N");
            var groupId = Guid.NewGuid().ToString();
            var schemaRegistryOptions = CreateSchemaRegistryOptions();

            using var queue = new KafkaSchemaRegistryPartitionQueue<SchemaRegistryAvroTestItem>(
                _bootstrapServers, topic, groupId, schemaRegistryOptions);

            await queue.OpenAsync(_cts.Token);

            var partitionKey = "avro-bootstrap-partition-key";
            var item = new SchemaRegistryAvroTestItem
            {
                Id = Guid.NewGuid().ToString(),
                Name = "Avro Bootstrap Partition Queue Test",
                Value = 555,
                CreatedAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            };

            await queue.EnqueueAsync(item, partitionKey, _cts.Token);

            var result = await queue.DequeueAsync(_cts.Token);

            Assert.NotNull(result);
            Assert.Equal(item.Id, result.Id);

            await queue.CloseAsync(_cts.Token);
        }

        public void Dispose()
        {
            _cts?.Dispose();
        }
    }
}

