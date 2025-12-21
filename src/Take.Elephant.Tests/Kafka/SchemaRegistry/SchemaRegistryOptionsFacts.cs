using Confluent.SchemaRegistry;
using Take.Elephant.Kafka.SchemaRegistry;
using Xunit;

namespace Take.Elephant.Tests.Kafka.SchemaRegistry
{
    [Trait("Category", nameof(Kafka))]
    public class SchemaRegistryOptionsFacts
    {
        [Fact]
        public void ConstructorWithUrlShouldInitializeWithJsonSerializer()
        {
            var options = new SchemaRegistryOptions("http://localhost:8081");

            Assert.NotNull(options.SchemaRegistryConfig);
            Assert.Equal("http://localhost:8081", options.SchemaRegistryConfig.Url);
            Assert.Equal(SchemaRegistrySerializerType.JSON, options.SerializerType);
        }

        [Fact]
        public void ConstructorWithSchemaRegistryConfigShouldWork()
        {
            var config = new SchemaRegistryConfig { Url = "http://localhost:8081" };
            var options = new SchemaRegistryOptions(config);

            Assert.Equal(config, options.SchemaRegistryConfig);
            Assert.Equal("http://localhost:8081", options.SchemaRegistryConfig.Url);
            Assert.Equal(SchemaRegistrySerializerType.JSON, options.SerializerType);
        }

        [Fact]
        public void ConstructorWithUrlAndSerializerTypeShouldWork()
        {
            var options = new SchemaRegistryOptions("http://localhost:8081", SchemaRegistrySerializerType.AVRO);

            Assert.Equal(SchemaRegistrySerializerType.AVRO, options.SerializerType);
        }

        [Fact]
        public void ConstructorWithConfigAndSerializerTypeShouldWork()
        {
            var config = new SchemaRegistryConfig { Url = "http://localhost:8081" };
            var options = new SchemaRegistryOptions(config, SchemaRegistrySerializerType.JSON);

            Assert.Equal(config, options.SchemaRegistryConfig);
            Assert.Equal(SchemaRegistrySerializerType.JSON, options.SerializerType);
        }

        [Fact]
        public void ShouldAllowSettingAvroSerializerConfig()
        {
            var config = new SchemaRegistryConfig
            {
                Url = "http://schema-registry:8081",
                BasicAuthUserInfo = "user:password"
            };

            var options = new SchemaRegistryOptions(config, SchemaRegistrySerializerType.AVRO);

            Assert.Equal(config, options.SchemaRegistryConfig);
            Assert.Equal(SchemaRegistrySerializerType.AVRO, options.SerializerType);
            Assert.Equal("http://schema-registry:8081", options.SchemaRegistryConfig.Url);
            Assert.Equal("user:password", options.SchemaRegistryConfig.BasicAuthUserInfo);
        }
    }
}
