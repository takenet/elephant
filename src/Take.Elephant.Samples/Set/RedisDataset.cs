using Take.Elephant.Redis;
using Take.Elephant.Redis.Serializers;

namespace Take.Elephant.Samples.Set
{
    public class RedisDataSet : RedisSet<Data>, IDataSet
    {
        public RedisDataSet()
            : base("data", "localhost", new ValueSerializer<Data>(), useScanOnEnumeration: true)
        {
        }
    }
}