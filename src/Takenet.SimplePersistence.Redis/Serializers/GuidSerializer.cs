using System;

namespace Takenet.SimplePersistence.Redis.Serializers
{
    public class GuidSerializer : ISerializer<Guid>
    {
        public string Serialize(Guid value)
        {
            return value.ToString();
        }

        public Guid Deserialize(string value)
        {
            return Guid.Parse(value);
        }
    }
}