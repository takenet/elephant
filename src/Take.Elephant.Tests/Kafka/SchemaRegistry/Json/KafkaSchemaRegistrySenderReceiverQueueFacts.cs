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
    public class KafkaSchemaRegistrySenderReceiverQueueFacts : IDisposable
    {
        private readonly string _bootstrapServers = "localhost:9092";
        private readonly string _schemaRegistryUrl = "http://localhost:8081";
        private readonly CancellationTokenSource _cts;

        public KafkaSchemaRegistrySenderReceiverQueueFacts()
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
            var topic = "test-schema-registry-sender-receiver-" + Guid.NewGuid().ToString("N");
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

            using var sender = new KafkaSchemaRegistrySenderQueue<SchemaRegistryTestItem>(
                producerConfig, topic, schemaRegistryOptions);
            using var receiver = new KafkaSchemaRegistryReceiverQueue<SchemaRegistryTestItem>(
                consumerConfig, topic, schemaRegistryOptions);

            var item = new SchemaRegistryTestItem
            {
                Id = Guid.NewGuid().ToString(),
                Name = "Sender Receiver Test",
                Value = 123,
                CreatedAt = DateTime.UtcNow
            };

            await sender.EnqueueAsync(item, _cts.Token);

            await receiver.OpenAsync(_cts.Token);

            var result = await receiver.DequeueAsync(_cts.Token);

            Assert.NotNull(result);
            Assert.Equal(item.Id, result.Id);
            Assert.Equal(item.Name, result.Name);
            Assert.Equal(item.Value, result.Value);

            await receiver.CloseAsync(_cts.Token);
        }

        [Fact]
        public async Task EnqueueAndDequeueMultipleItemsShouldSucceed()
        {
            var topic = "test-schema-registry-sender-receiver-multi-" + Guid.NewGuid().ToString("N");
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

            using var sender = new KafkaSchemaRegistrySenderQueue<SchemaRegistryTestItem>(
                producerConfig, topic, schemaRegistryOptions);
            using var receiver = new KafkaSchemaRegistryReceiverQueue<SchemaRegistryTestItem>(
                consumerConfig, topic, schemaRegistryOptions);

            var items = Enumerable.Range(1, 10).Select(i => new SchemaRegistryTestItem
            {
                Id = Guid.NewGuid().ToString(),
                Name = $"Multi Item {i}",
                Value = i,
                CreatedAt = DateTime.UtcNow
            }).ToList();

            foreach (var item in items)
            {
                await sender.EnqueueAsync(item, _cts.Token);
            }

            await receiver.OpenAsync(_cts.Token);

            var receivedItems = new List<SchemaRegistryTestItem>();
            for (int i = 0; i < items.Count; i++)
            {
                var result = await receiver.DequeueAsync(_cts.Token);
                if (result != null)
                {
                    receivedItems.Add(result);
                }
            }

            Assert.Equal(items.Count, receivedItems.Count);

            await receiver.CloseAsync(_cts.Token);
        }

        [Fact]
        public async Task SenderWithBootstrapServerConstructorShouldSucceed()
        {
            var topic = "test-schema-registry-sender-bootstrap-" + Guid.NewGuid().ToString("N");
            var schemaRegistryOptions = CreateSchemaRegistryOptions();

            using var sender = new KafkaSchemaRegistrySenderQueue<SchemaRegistryTestItem>(
                _bootstrapServers, topic, schemaRegistryOptions);

            var item = new SchemaRegistryTestItem
            {
                Id = Guid.NewGuid().ToString(),
                Name = "Bootstrap Test",
                Value = 999,
                CreatedAt = DateTime.UtcNow
            };

            await sender.EnqueueAsync(item, _cts.Token);
        }

        [Fact]
        public async Task ReceiverWithBootstrapServerConstructorShouldSucceed()
        {
            var topic = "test-schema-registry-receiver-bootstrap-" + Guid.NewGuid().ToString("N");
            var groupId = Guid.NewGuid().ToString();
            var schemaRegistryOptions = CreateSchemaRegistryOptions();

            using var receiver = new KafkaSchemaRegistryReceiverQueue<SchemaRegistryTestItem>(
                _bootstrapServers, topic, groupId, schemaRegistryOptions);

            await receiver.OpenAsync(_cts.Token);

            var result = await receiver.DequeueOrDefaultAsync(_cts.Token);

            Assert.Null(result);

            await receiver.CloseAsync(_cts.Token);
        }

        [Fact]
        public async Task SenderReceiverWithSharedSchemaRegistryClientShouldSucceed()
        {
            var topic = "test-schema-registry-shared-client-" + Guid.NewGuid().ToString("N");
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
            using var sender = new KafkaSchemaRegistrySenderQueue<SchemaRegistryTestItem>(
                producerConfig, topic, schemaRegistryClient, schemaRegistryOptions);
            using var receiver = new KafkaSchemaRegistryReceiverQueue<SchemaRegistryTestItem>(
                consumerConfig, topic, schemaRegistryClient, schemaRegistryOptions);

            var item = new SchemaRegistryTestItem
            {
                Id = Guid.NewGuid().ToString(),
                Name = "Shared Client Test",
                Value = 777,
                CreatedAt = DateTime.UtcNow
            };

            await sender.EnqueueAsync(item, _cts.Token);

            await receiver.OpenAsync(_cts.Token);

            var result = await receiver.DequeueAsync(_cts.Token);

            Assert.NotNull(result);
            Assert.Equal(item.Id, result.Id);

            await receiver.CloseAsync(_cts.Token);
        }

        public void Dispose()
        {
            _cts?.Dispose();
        }
    }
}
