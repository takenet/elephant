using Avro;
using Avro.Specific;
using System;

namespace Take.Elephant.Tests.Kafka.SchemaRegistry.Avro
{
    public class SchemaRegistryAvroTestItem : ISpecificRecord
    {
        private static readonly Schema _schema = Schema.Parse(
            """
            {
                "type": "record",
                "name": "SchemaRegistryAvroTestItem",
                "namespace": "Take.Elephant.Tests.Kafka.SchemaRegistry",
                "fields": [
                    { "name": "Id", "type": ["null", "string"], "default": null },
                    { "name": "Name", "type": ["null", "string"], "default": null },
                    { "name": "Value", "type": "int" },
                    { "name": "CreatedAt", "type": "long" }
                ]
            }
            """);

        public string Id { get; set; }
        public string Name { get; set; }
        public int Value { get; set; }
        public long CreatedAt { get; set; }

        public Schema Schema => _schema;

        public object Get(int fieldPos)
        {
            return fieldPos switch
            {
                0 => Id,
                1 => Name,
                2 => Value,
                3 => CreatedAt,
                _ => throw new AvroRuntimeException($"Bad index {fieldPos} in Get()")
            };
        }

        public void Put(int fieldPos, object fieldValue)
        {
            switch (fieldPos)
            {
                case 0:
                    Id = (string)fieldValue;
                    break;
                case 1:
                    Name = (string)fieldValue;
                    break;
                case 2:
                    Value = (int)fieldValue;
                    break;
                case 3:
                    CreatedAt = (long)fieldValue;
                    break;
                default:
                    throw new AvroRuntimeException($"Bad index {fieldPos} in Put()");
            }
        }

        public override bool Equals(object obj)
        {
            if (obj is SchemaRegistryAvroTestItem other)
            {
                return Id == other.Id &&
                       Name == other.Name &&
                       Value == other.Value;
            }

            return false;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Id, Name, Value);
        }
    }
}