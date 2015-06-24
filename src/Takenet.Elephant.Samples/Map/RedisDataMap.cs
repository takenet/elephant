using System;
using Takenet.Elephant.Redis;
using Takenet.Elephant.Redis.Converters;

namespace Takenet.Elephant.Samples.Map
{
    public class RedisDataMap : RedisHashMap<Guid, Data>, IDataMap
    {
        public RedisDataMap() 
            : base("data", new TypeRedisDictionaryConverter<Data>(), "localhost")
        {

        }
    }
}