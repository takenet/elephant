using Newtonsoft.Json;

namespace Take.Elephant.Benchmark.Kafka
{
    public class JsonItemSerializer : ISerializer<Item>
    {
        public Item Deserialize(string value)
        {
            return JsonConvert.DeserializeObject<Item>(value);
        }

        public string Serialize(Item value)
        {
            return JsonConvert.SerializeObject(value);
        }
    }
}