using System.Threading.Tasks;
using StackExchange.Redis;

namespace Takenet.Elephant.Redis
{
    public class RedisNumberMap<TKey> : RedisStringMap<TKey, long>, INumberMap<TKey>
    {
        public RedisNumberMap(string mapName, string configuration, ISerializer<long> serializer, int db = 0) 
            : base(mapName, configuration, serializer, db)
        {
        }

        internal RedisNumberMap(string mapName, ConnectionMultiplexer connectionMultiplexer, ISerializer<long> serializer, int db) 
            : base(mapName, connectionMultiplexer, serializer, db)
        {
        }

        public Task<long> IncrementAsync(TKey key)
        {
            return GetDatabase().StringIncrementAsync(GetRedisKey(key));
        }

        public Task<long> IncrementAsync(TKey key, long value)
        {
            return GetDatabase().StringIncrementAsync(GetRedisKey(key), value);
        }

        public Task<long> DecrementAsync(TKey key)
        {
            return GetDatabase().StringDecrementAsync(GetRedisKey(key));
        }

        public Task<long> DecrementAsync(TKey key, long value)
        {
            return GetDatabase().StringDecrementAsync(GetRedisKey(key), value);
        }  
    }
}
