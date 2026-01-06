using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace Take.Elephant.Kafka.SchemaRegistry
{
    /// <summary>
    /// Factory class for creating Schema Registry serializers and deserializers.
    /// </summary>
    public static class SchemaRegistrySerializerFactory
    {
        /// <summary>
        /// Creates an async serializer for the specified type using Schema Registry.
        /// </summary>
        /// <typeparam name="T">The type to serialize.</typeparam>
        /// <param name="schemaRegistryClient">The Schema Registry client.</param>
        /// <param name="options">The Schema Registry options.</param>
        /// <returns>A Kafka async serializer for the specified type.</returns>
        public static IAsyncSerializer<T> CreateSerializer<T>(
            ISchemaRegistryClient schemaRegistryClient,
            SchemaRegistryOptions options) where T : class
        {
            if (schemaRegistryClient == null)
                throw new ArgumentNullException(nameof(schemaRegistryClient));
            if (options == null)
                throw new ArgumentNullException(nameof(options));

            return options.SerializerType switch
            {
                SchemaRegistrySerializerType.AVRO => new AvroSerializer<T>(schemaRegistryClient, options.AvroSerializerConfig),
                SchemaRegistrySerializerType.JSON => new JsonSerializer<T>(schemaRegistryClient, options.JsonSerializerConfig),
                _ => throw new ArgumentOutOfRangeException(nameof(options), $"Unsupported serializer type: {options.SerializerType}")
            };
        }

        /// <summary>
        /// Creates an async deserializer for the specified type using Schema Registry.
        /// </summary>
        /// <typeparam name="T">The type to deserialize.</typeparam>
        /// <param name="schemaRegistryClient">The Schema Registry client.</param>
        /// <param name="options">The Schema Registry options.</param>
        /// <returns>A Kafka async deserializer for the specified type.</returns>
        public static IAsyncDeserializer<T> CreateAsyncDeserializer<T>(
            ISchemaRegistryClient schemaRegistryClient,
            SchemaRegistryOptions options) where T : class
        {
            if (schemaRegistryClient == null)
                throw new ArgumentNullException(nameof(schemaRegistryClient));
            if (options == null)
                throw new ArgumentNullException(nameof(options));

            return options.SerializerType switch
            {
                SchemaRegistrySerializerType.AVRO => new AvroDeserializer<T>(schemaRegistryClient, options.AvroDeserializerConfig),
                SchemaRegistrySerializerType.JSON => new Confluent.SchemaRegistry.Serdes.JsonDeserializer<T>(schemaRegistryClient, options.JsonDeserializerConfig ?? new JsonDeserializerConfig()),
                _ => throw new ArgumentOutOfRangeException(nameof(options), $"Unsupported deserializer type: {options.SerializerType}")
            };
        }

        /// <summary>
        /// Creates a sync deserializer for the specified type using Schema Registry.
        /// This wraps an async deserializer using <see cref="SyncOverAsyncDeserializer{T}"/>.
        /// </summary>
        /// <typeparam name="T">The type to deserialize.</typeparam>
        /// <param name="schemaRegistryClient">The Schema Registry client.</param>
        /// <param name="options">The Schema Registry options.</param>
        /// <returns>A Kafka sync deserializer for the specified type.</returns>
        public static IDeserializer<T> CreateDeserializer<T>(
            ISchemaRegistryClient schemaRegistryClient,
            SchemaRegistryOptions options) where T : class
        {
            var asyncDeserializer = CreateAsyncDeserializer<T>(schemaRegistryClient, options);
            return new SyncOverAsyncDeserializer<T>(asyncDeserializer);
        }
    }
}
