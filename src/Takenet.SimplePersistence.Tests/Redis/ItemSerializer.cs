using Takenet.SimplePersistence.Redis;

namespace Takenet.SimplePersistence.Tests.Redis
{
    public class ItemSerializer : ISerializer<Item>
    {
        public string Serialize(Item value)
        {
            return value.ToString();
        }

        public Item Deserialize(string value)
        {
            return Item.Parse(value);
        }
    }
}