using System.Threading.Tasks;
using StackExchange.Redis;

namespace Take.Elephant.Redis
{
    public class RedisNumberMap<TKey> : RedisStringMap<TKey, long>, INumberMap<TKey>
    {
        public RedisNumberMap(string mapName, string configuration, ISerializer<long> serializer, int db = 0, CommandFlags readFlags = CommandFlags.None, CommandFlags writeFlags = CommandFlags.None) 
            : base(mapName, configuration, serializer, db, readFlags, writeFlags)
        {
        }

        public RedisNumberMap(string mapName, IConnectionMultiplexer connectionMultiplexer, ISerializer<long> serializer, int db = 0, CommandFlags readFlags = CommandFlags.None, CommandFlags writeFlags = CommandFlags.None)
            : base(mapName, connectionMultiplexer, serializer, db, readFlags, writeFlags)
        {
        }

        public virtual Task<long> IncrementAsync(TKey key)
        {
            return GetDatabase().StringIncrementAsync(GetRedisKey(key), flags: WriteFlags);
        }

        public virtual Task<long> IncrementAsync(TKey key, long value)
        {
            return GetDatabase().StringIncrementAsync(GetRedisKey(key), value, WriteFlags);
        }

        public virtual Task<long> DecrementAsync(TKey key)
        {
            return GetDatabase().StringDecrementAsync(GetRedisKey(key), flags: WriteFlags);
        }

        public virtual Task<long> DecrementAsync(TKey key, long value)
        {
            return GetDatabase().StringDecrementAsync(GetRedisKey(key), value, WriteFlags);
        }  
    }
}
