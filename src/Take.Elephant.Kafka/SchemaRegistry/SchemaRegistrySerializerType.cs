namespace Take.Elephant.Kafka.SchemaRegistry
{
    /// <summary>
    /// Defines the type of serialization to use with Schema Registry.
    /// </summary>
    public enum SchemaRegistrySerializerType
    {
        /// <summary>
        /// Uses Avro serialization format.
        /// </summary>
        AVRO,

        /// <summary>
        /// Uses JSON Schema serialization format.
        /// </summary>
        JSON
    }
}

