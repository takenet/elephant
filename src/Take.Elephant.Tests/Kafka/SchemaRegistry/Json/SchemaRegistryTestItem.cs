using System;

namespace Take.Elephant.Tests.Kafka.SchemaRegistry.Json
{
    public class SchemaRegistryTestItem
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public int Value { get; set; }
        public DateTime CreatedAt { get; set; }

        public override bool Equals(object obj)
        {
            if (obj is SchemaRegistryTestItem other)
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

