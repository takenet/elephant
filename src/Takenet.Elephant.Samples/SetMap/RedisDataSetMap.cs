using System;
using Takenet.Elephant.Redis;
using Takenet.Elephant.Redis.Serializers;

namespace Takenet.Elephant.Samples.SetMap
{
    public class RedisDataSetMap : RedisSetMap<Guid, Data>, IDataSetMap
    {
        public RedisDataSetMap() 
            : base("guid-data", "localhost", new ValueSerializer<Data>())
        {
        }
    }
}