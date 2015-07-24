using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Takenet.Elephant.Redis
{
    public abstract class MapBase<TKey, TValue> : StorageBase<TKey>, IExpirableKeyMap<TKey, TValue>, IKeysMap<TKey, TValue>
    {
        protected MapBase(string mapName, string configuration, int db)
            : base(mapName, configuration, db)
        {

        }

        protected MapBase(string mapName, ConnectionMultiplexer connectionMultiplexer, int db)
            : base(mapName, connectionMultiplexer, db)
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

        protected virtual TKey GetKeyFromString(string value)
        {
            return TypeUtil.GetParseFunc<TKey>()(value);
        }

        public virtual Task<IAsyncEnumerable<TKey>> GetKeysAsync()
        {
            var endpoint = _connectionMultiplexer.GetEndPoints(true).FirstOrDefault();
            if (endpoint == null) throw new InvalidOperationException("There's no connection endpoints available");

            var server = _connectionMultiplexer.GetServer(endpoint);
            var cursor = server.Keys(_db, $"{_name}:*");       
            var keys = cursor.Select(k =>
            {
                var value = (string) k;
                var key = ((string) k).Substring(_name.Length + 1, value.Length - _name.Length - 1);
                return GetKeyFromString(key);
            });
            return Task.FromResult<IAsyncEnumerable<TKey>>(
                new AsyncEnumerableWrapper<TKey>(keys));
        }
    }
}
