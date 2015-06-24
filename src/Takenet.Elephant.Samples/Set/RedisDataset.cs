using Takenet.Elephant.Redis;
using Takenet.Elephant.Redis.Serializers;

namespace Takenet.Elephant.Samples.Set
{
    public class RedisDataSet : RedisSet<Data>, IDataSet
    {
        public RedisDataSet()
            : base("data", "localhost", new ValueSerializer<Data>(), useScanOnEnumeration: true)
        {
        }
    }
}