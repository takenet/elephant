using System;

namespace Takenet.Elephant.Tests.Specialized
{
    public class GuidSerializer : ISerializer<Guid>
    {
        public string Serialize(Guid value)
        {
            return value.ToString();
        }

        public Guid Deserialize(string value)
        {
            return new Guid(value);
        }
    }
}