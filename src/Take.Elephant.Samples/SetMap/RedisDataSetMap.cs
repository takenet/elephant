using System;
using Take.Elephant.Redis;
using Take.Elephant.Redis.Serializers;

namespace Take.Elephant.Samples.SetMap
{
    public class RedisDataSetMap : RedisSetMap<Guid, Data>, IDataSetMap
    {
        public RedisDataSetMap() 
            : base("guid-data", "localhost", new ValueSerializer<Data>())
        {
        }
    }
}