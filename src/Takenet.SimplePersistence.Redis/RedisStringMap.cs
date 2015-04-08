using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Takenet.SimplePersistence.Redis
{
    /// <summary>
    /// Implements the <see cref="IMap{TKey,TValue}"/> interface using Redis stardard key/value data structure.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public class RedisStringMap<TKey, TValue> : MapBase<TKey, TValue>
    {
        protected readonly ISerializer<TValue> _serializer;

        public RedisStringMap(string mapName, string configuration, ISerializer<TValue> serializer)
            : base(mapName, configuration)
        {
            if (serializer == null)
            {
                throw new ArgumentNullException("serializer");
            }

            _serializer = serializer;
        }

        internal RedisStringMap(string mapName, ConnectionMultiplexer connectionMultiplexer, ISerializer<TValue> serializer)
            : base(mapName, connectionMultiplexer)
        {
            if (serializer == null)
            {
                throw new ArgumentNullException("serializer");
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
                when: overwrite ? When.Always : When.NotExists);
        }

        public override async Task<TValue> GetValueOrDefaultAsync(TKey key)
        {
            var database = GetDatabase();
            var redisValue = await database.StringGetAsync(GetRedisKey(key));
            return redisValue.IsNull ? default(TValue) : _serializer.Deserialize(redisValue);
        }

        public override Task<bool> TryRemoveAsync(TKey key)
        {
            var database = GetDatabase();
            return database.KeyDeleteAsync(GetRedisKey(key));
        }

        public override Task<bool> ContainsKeyAsync(TKey key)
        {
            var database = GetDatabase();
            return database.KeyExistsAsync(GetRedisKey(key));
        }

        #endregion
    }
}
