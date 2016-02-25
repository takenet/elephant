using System;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Takenet.Elephant.Redis
{
    /// <summary>
    /// Implements the <see cref="IMap{TKey,TValue}"/> interface using Redis standard key/value data structure.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public class RedisStringMap<TKey, TValue> : MapBase<TKey, TValue>
    {
        protected readonly ISerializer<TValue> _serializer;

        public RedisStringMap(string mapName, string configuration, ISerializer<TValue> serializer, int db = 0)
            : this(mapName, ConnectionMultiplexer.Connect(configuration), serializer, db)
        {
        
        }

        public RedisStringMap(string mapName, IConnectionMultiplexer connectionMultiplexer, ISerializer<TValue> serializer, int db)
            : base(mapName, connectionMultiplexer, db)
        {
            if (serializer == null)
            {
                throw new ArgumentNullException(nameof(serializer));
            }

            _serializer = serializer;
        }

        #region IMap<TKey,TValue> Members

        public override Task<bool> TryAddAsync(TKey key, TValue value, bool overwrite = false)
        {
            var database = GetDatabase();
            return database.StringSetAsync(
                GetRedisKey(key),
                _serializer.Serialize(value),
                when: overwrite ? When.Always : When.NotExists,
                flags: GetFlags());
        }

        public override async Task<TValue> GetValueOrDefaultAsync(TKey key)
        {
            var database = GetDatabase();
            var redisValue = await database.StringGetAsync(GetRedisKey(key), GetFlags());
            return redisValue.IsNull ? default(TValue) : _serializer.Deserialize(redisValue);
        }

        public override Task<bool> TryRemoveAsync(TKey key)
        {
            var database = GetDatabase();
            return database.KeyDeleteAsync(GetRedisKey(key), GetFlags());
        }

        public override Task<bool> ContainsKeyAsync(TKey key)
        {
            var database = GetDatabase();
            return database.KeyExistsAsync(GetRedisKey(key), GetFlags());
        }

        #endregion
    }
}
