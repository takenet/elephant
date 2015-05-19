using System.Threading.Tasks;
using StackExchange.Redis;

namespace Takenet.Elephant.Redis
{
    public class RedisNumberMap<TKey> : RedisStringMap<TKey, long>, INumberMap<TKey>
    {
        public RedisNumberMap(string mapName, string configuration, ISerializer<long> serializer) 
            : base(mapName, configuration, serializer)
        {
        }

        internal RedisNumberMap(string mapName, ConnectionMultiplexer connectionMultiplexer, ISerializer<long> serializer) 
            : base(mapName, connectionMultiplexer, serializer)
        {
        }

        public Task<long> IncrementAsync(TKey key)
        {
            var database = _connectionMultiplexer.GetDatabase();
            return database.StringIncrementAsync(GetRedisKey(key));
        }

        public Task<long> IncrementAsync(TKey key, long value)
        {
            var database = _connectionMultiplexer.GetDatabase();
            return database.StringIncrementAsync(GetRedisKey(key), value);
        }

        public Task<long> DecrementAsync(TKey key)
        {
            var database = _connectionMultiplexer.GetDatabase();
            return database.StringDecrementAsync(GetRedisKey(key));
        }

        public Task<long> DecrementAsync(TKey key, long value)
        {
            var database = _connectionMultiplexer.GetDatabase();
            return database.StringDecrementAsync(GetRedisKey(key), value);
        }  
    }
}
