using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Takenet.SimplePersistence.Redis
{
    public abstract class MapBase<TKey, TValue> : StorageBase<TKey>, IExpirableKeyMap<TKey, TValue>
    {
        protected MapBase(string name, string configuration)
            : base(name, configuration)
        {

        }

        protected MapBase(string name, ConnectionMultiplexer connectionMultiplexer)
            : base(name, connectionMultiplexer)
        {

        }

        public abstract Task<bool> TryAddAsync(TKey key, TValue value, bool overwrite = false);
        public abstract Task<TValue> GetValueOrDefaultAsync(TKey key);
        public abstract Task<bool> TryRemoveAsync(TKey key);
        public abstract Task<bool> ContainsKeyAsync(TKey key);

        public virtual async Task SetRelativeKeyExpirationAsync(TKey key, TimeSpan ttl)
        {
            var database = _connectionMultiplexer.GetDatabase();
            if (!await database.KeyExpireAsync(GetRedisKey(key), ttl).ConfigureAwait(false))
            {
                throw new ArgumentException("Invalid key");
            }
        }

        public virtual async Task SetAbsoluteKeyExpirationAsync(TKey key, DateTimeOffset expiration)
        {
            var database = _connectionMultiplexer.GetDatabase();
            if (!await database.KeyExpireAsync(GetRedisKey(key), expiration.UtcDateTime).ConfigureAwait(false))
            {
                throw new ArgumentException("Invalid key");
            }
        }
    }
}
