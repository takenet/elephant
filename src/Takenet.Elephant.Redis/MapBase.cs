using System;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Takenet.Elephant.Redis
{
    public abstract class MapBase<TKey, TValue> : StorageBase<TKey>, IExpirableKeyMap<TKey, TValue>
    {
        protected MapBase(string mapName, string configuration)
            : base(mapName, configuration)
        {

        }

        protected MapBase(string mapName, ConnectionMultiplexer connectionMultiplexer)
            : base(mapName, connectionMultiplexer)
        {

        }

        public abstract Task<bool> TryAddAsync(TKey key, TValue value, bool overwrite = false);
        public abstract Task<TValue> GetValueOrDefaultAsync(TKey key);
        public abstract Task<bool> TryRemoveAsync(TKey key);
        public abstract Task<bool> ContainsKeyAsync(TKey key);
        
        #region IExpirableKeyMap<TKey, TValue> Members

        public virtual async Task SetRelativeKeyExpirationAsync(TKey key, TimeSpan ttl)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            var database = GetDatabase();
            if (!await database.KeyExpireAsync(GetRedisKey(key), ttl).ConfigureAwait(false))
            {
                throw new ArgumentException("Invalid key", nameof(key));
            }
        }

        public virtual async Task SetAbsoluteKeyExpirationAsync(TKey key, DateTimeOffset expiration)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            var database = GetDatabase();
            if (!await database.KeyExpireAsync(GetRedisKey(key), expiration.UtcDateTime).ConfigureAwait(false))
            {
                throw new ArgumentException("Invalid key", nameof(key));
            }
        }

        #endregion
    }
}
