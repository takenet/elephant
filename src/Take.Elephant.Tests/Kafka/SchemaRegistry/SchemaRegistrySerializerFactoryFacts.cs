using Confluent.Kafka;
using Confluent.SchemaRegistry;
using System;
using Take.Elephant.Kafka.SchemaRegistry;
using Take.Elephant.Tests.Kafka.SchemaRegistry.Json;
using Xunit;

namespace Take.Elephant.Tests.Kafka.SchemaRegistry
{
    [Trait("Category", nameof(Kafka))]
    public class SchemaRegistrySerializerFactoryFacts : IDisposable
    {
        private readonly string _schemaRegistryUrl = "http://localhost:8081";
        private readonly ISchemaRegistryClient _schemaRegistryClient;

        public SchemaRegistrySerializerFactoryFacts()
        {
            var config = new SchemaRegistryConfig { Url = _schemaRegistryUrl };
            _schemaRegistryClient = new CachedSchemaRegistryClient(config);
        }

        [Fact]
        public void CreateSerializerWithJsonTypeShouldReturnSerializer()
        {
            var options = new SchemaRegistryOptions(_schemaRegistryUrl, SchemaRegistrySerializerType.JSON);

            var serializer = SchemaRegistrySerializerFactory.CreateSerializer<SchemaRegistryTestItem>(
                _schemaRegistryClient, options);

            Assert.NotNull(serializer);
        }

        [Fact]
        public void CreateSerializerWithAvroTypeShouldReturnSerializer()
        {
            var options = new SchemaRegistryOptions(_schemaRegistryUrl, SchemaRegistrySerializerType.AVRO);

            var serializer = SchemaRegistrySerializerFactory.CreateSerializer<SchemaRegistryTestItem>(
                _schemaRegistryClient, options);

            Assert.NotNull(serializer);
        }

        [Fact]
        public void CreateDeserializerWithJsonTypeShouldReturnDeserializer()
        {
            var options = new SchemaRegistryOptions(_schemaRegistryUrl, SchemaRegistrySerializerType.JSON);

            var deserializer = SchemaRegistrySerializerFactory.CreateDeserializer<SchemaRegistryTestItem>(
                _schemaRegistryClient, options);

            Assert.NotNull(deserializer);
        }

        [Fact]
        public void CreateDeserializerWithAvroTypeShouldReturnDeserializer()
        {
            var options = new SchemaRegistryOptions(_schemaRegistryUrl, SchemaRegistrySerializerType.AVRO);

            var deserializer = SchemaRegistrySerializerFactory.CreateDeserializer<SchemaRegistryTestItem>(
                _schemaRegistryClient, options);

            Assert.NotNull(deserializer);
        }

        [Fact]
        public void CreateSerializerWithNullClientShouldThrowArgumentNullException()
        {
            var options = new SchemaRegistryOptions(_schemaRegistryUrl, SchemaRegistrySerializerType.JSON);

            Assert.Throws<ArgumentNullException>(() =>
                SchemaRegistrySerializerFactory.CreateSerializer<SchemaRegistryTestItem>(null, options));
        }

        [Fact]
        public void CreateSerializerWithNullOptionsShouldThrowArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() =>
                SchemaRegistrySerializerFactory.CreateSerializer<SchemaRegistryTestItem>(_schemaRegistryClient, null));
        }

        [Fact]
        public void CreateDeserializerWithNullClientShouldThrowArgumentNullException()
        {
            var options = new SchemaRegistryOptions(_schemaRegistryUrl, SchemaRegistrySerializerType.JSON);

            Assert.Throws<ArgumentNullException>(() =>
                SchemaRegistrySerializerFactory.CreateDeserializer<SchemaRegistryTestItem>(null, options));
        }

        [Fact]
        public void CreateDeserializerWithNullOptionsShouldThrowArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() =>
                SchemaRegistrySerializerFactory.CreateDeserializer<SchemaRegistryTestItem>(_schemaRegistryClient, null));
        }

        public void Dispose()
        {
            _schemaRegistryClient?.Dispose();
        }
    }
}
