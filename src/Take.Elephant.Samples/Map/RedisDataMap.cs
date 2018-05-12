using System;
using Take.Elephant.Redis;
using Take.Elephant.Redis.Converters;

namespace Take.Elephant.Samples.Map
{
    public class RedisDataMap : RedisHashMap<Guid, Data>, IDataMap
    {
        public RedisDataMap() 
            : base("data", new TypeRedisDictionaryConverter<Data>(), "localhost")
        {

        }
    }
}