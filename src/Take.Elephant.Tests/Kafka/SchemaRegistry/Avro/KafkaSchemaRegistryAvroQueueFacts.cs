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
    public class KafkaSchemaRegistryAvroQueueFacts : IDisposable
    {
        private readonly string _bootstrapServers = "localhost:9092";
        private readonly string _schemaRegistryUrl = "http://localhost:8081";
        private readonly CancellationTokenSource _cts;

        public KafkaSchemaRegistryAvroQueueFacts()
        {
            _cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        }

        private SchemaRegistryOptions CreateSchemaRegistryOptions()
        {
            return new SchemaRegistryOptions(_schemaRegistryUrl, SchemaRegistrySerializerType.AVRO);
        }

        [Fact]
        public async Task EnqueueAndDequeueSingleItemShouldSucceed()
        {
            var topic = "test-avro-queue-" + Guid.NewGuid().ToString("N");
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

            using var queue = new KafkaSchemaRegistryQueue<SchemaRegistryAvroTestItem>(
                producerConfig, consumerConfig, topic, schemaRegistryOptions);

            var item = new SchemaRegistryAvroTestItem
            {
                Id = Guid.NewGuid().ToString(),
                Name = "Avro Queue Test",
                Value = 500,
                CreatedAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
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
            var topic = "test-avro-queue-multi-" + Guid.NewGuid().ToString("N");
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

            using var queue = new KafkaSchemaRegistryQueue<SchemaRegistryAvroTestItem>(
                producerConfig, consumerConfig, topic, schemaRegistryOptions);

            var items = Enumerable.Range(1, 5).Select(i => new SchemaRegistryAvroTestItem
            {
                Id = Guid.NewGuid().ToString(),
                Name = $"Avro Queue Item {i}",
                Value = i * 100,
                CreatedAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            }).ToList();

            foreach (var item in items)
            {
                await queue.EnqueueAsync(item, _cts.Token);
            }

            var receivedItems = new List<SchemaRegistryAvroTestItem>();
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
            var topic = "test-avro-queue-bootstrap-" + Guid.NewGuid().ToString("N");
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

            using var queue = new KafkaSchemaRegistryQueue<SchemaRegistryAvroTestItem>(
                producerConfig, consumerConfig, topic, schemaRegistryOptions);

            var item = new SchemaRegistryAvroTestItem
            {
                Id = Guid.NewGuid().ToString(),
                Name = "Avro Bootstrap Queue Test",
                Value = 321,
                CreatedAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
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
            var topic = "test-avro-queue-shared-" + Guid.NewGuid().ToString("N");
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
            using var queue = new KafkaSchemaRegistryQueue<SchemaRegistryAvroTestItem>(
                producerConfig, consumerConfig, topic, schemaRegistryClient, schemaRegistryOptions);

            var item = new SchemaRegistryAvroTestItem
            {
                Id = Guid.NewGuid().ToString(),
                Name = "Avro Shared Client Queue Test",
                Value = 654,
                CreatedAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
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
            var topic = "test-avro-queue-empty-" + Guid.NewGuid().ToString("N");
            var groupId = Guid.NewGuid().ToString();
            var schemaRegistryOptions = CreateSchemaRegistryOptions();

            using var queue = new KafkaSchemaRegistryQueue<SchemaRegistryAvroTestItem>(
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

