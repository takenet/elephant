using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace Take.Elephant.Kafka.SchemaRegistry
{
    /// <summary>
    /// Configuration options for Schema Registry serialization/deserialization.
    /// </summary>
    public class SchemaRegistryOptions
    {
        /// <summary>
        /// Creates a new instance of <see cref="SchemaRegistryOptions"/>.
        /// </summary>
        /// <param name="schemaRegistryUrl">The Schema Registry URL.</param>
        /// <param name="serializerType">The serializer type to use.</param>
        public SchemaRegistryOptions(string schemaRegistryUrl, SchemaRegistrySerializerType serializerType = SchemaRegistrySerializerType.JSON)
            : this(new SchemaRegistryConfig { Url = schemaRegistryUrl }, serializerType)
        {
        }

        /// <summary>
        /// Creates a new instance of <see cref="SchemaRegistryOptions"/>.
        /// </summary>
        /// <param name="schemaRegistryConfig">The Schema Registry configuration.</param>
        /// <param name="serializerType">The serializer type to use.</param>
        public SchemaRegistryOptions(SchemaRegistryConfig schemaRegistryConfig, SchemaRegistrySerializerType serializerType = SchemaRegistrySerializerType.JSON)
        {
            SchemaRegistryConfig = schemaRegistryConfig;
            SerializerType = serializerType;
        }

        /// <summary>
        /// Gets the Schema Registry configuration.
        /// </summary>
        public SchemaRegistryConfig SchemaRegistryConfig { get; }

        /// <summary>
        /// Gets the serializer type to use.
        /// </summary>
        public SchemaRegistrySerializerType SerializerType { get; }

        /// <summary>
        /// Gets or sets the Avro serializer configuration.
        /// Only used when <see cref="SerializerType"/> is <see cref="SchemaRegistrySerializerType.AVRO"/>.
        /// </summary>
        public AvroSerializerConfig AvroSerializerConfig { get; set; }

        /// <summary>
        /// Gets or sets the Avro deserializer configuration.
        /// Only used when <see cref="SerializerType"/> is <see cref="SchemaRegistrySerializerType.AVRO"/>.
        /// </summary>
        public AvroDeserializerConfig AvroDeserializerConfig { get; set; }

        /// <summary>
        /// Gets or sets the JSON serializer configuration.
        /// Only used when <see cref="SerializerType"/> is <see cref="SchemaRegistrySerializerType.JSON"/>.
        /// </summary>
        public JsonSerializerConfig JsonSerializerConfig { get; set; }

        /// <summary>
        /// Gets or sets the JSON deserializer configuration.
        /// Only used when <see cref="SerializerType"/> is <see cref="SchemaRegistrySerializerType.JSON"/>.
        /// </summary>
        public JsonDeserializerConfig JsonDeserializerConfig { get; set; }

        /// <summary>
        /// Gets or sets the JSON schema for deserialization.
        /// Only used when <see cref="SerializerType"/> is <see cref="SchemaRegistrySerializerType.JSON"/>.
        /// Required when using external schema references.
        /// </summary>
        public Schema JsonSchema { get; set; }
    }
}
