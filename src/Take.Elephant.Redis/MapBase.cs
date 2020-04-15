using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Take.Elephant.Redis
{
    public abstract class MapBase<TKey, TValue> : StorageBase<TKey>, IExpirableKeyMap<TKey, TValue>, IKeysMap<TKey, TValue>
    {
        protected MapBase(string mapName, string configuration, int db, CommandFlags readFlags, CommandFlags writeFlags)
            : base(mapName, configuration, db, readFlags, writeFlags)
        {

        }

        protected MapBase(string mapName, IConnectionMultiplexer connectionMultiplexer, int db, CommandFlags readFlags, CommandFlags writeFlags)
            : base(mapName, connectionMultiplexer, db, readFlags, writeFlags)
        {

        }

        public abstract Task<bool> TryAddAsync(TKey key,
            TValue value,
            bool overwrite = false,
            CancellationToken cancellationToken = default);

        public abstract Task<TValue> GetValueOrDefaultAsync(TKey key, CancellationToken cancellationToken = default);

        public abstract Task<bool> TryRemoveAsync(TKey key, CancellationToken cancellationToken = default);

        public abstract Task<bool> ContainsKeyAsync(TKey key, CancellationToken cancellationToken = default);
        
        #region IExpirableKeyMap<TKey, TValue> Members

        public virtual Task<bool> SetRelativeKeyExpirationAsync(TKey key, TimeSpan ttl)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            var database = GetDatabase();
            return database.KeyExpireAsync(GetRedisKey(key), ttl, WriteFlags);
        }

        public virtual Task<bool> SetAbsoluteKeyExpirationAsync(TKey key, DateTimeOffset expiration)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            var database = GetDatabase();
            return database.KeyExpireAsync(GetRedisKey(key), expiration.UtcDateTime, WriteFlags);
        }

        #endregion

        public virtual Task<IAsyncEnumerable<TKey>> GetKeysAsync()
        {
            var endpoint = ConnectionMultiplexer.GetEndPoints(true).FirstOrDefault();
            if (endpoint == null) throw new InvalidOperationException("There's no connection endpoints available");

            var server = ConnectionMultiplexer.GetServer(endpoint);
            var cursor = server.Keys(Db, $"{Name}:*", flags: ReadFlags);       
            var keys = cursor.Select(k =>
            {
                var value = (string) k;
                var key = ((string) k).Substring(Name.Length + 1, value.Length - Name.Length - 1);
                return GetKeyFromString(key);
            });
            return Task.FromResult<IAsyncEnumerable<TKey>>(
                new AsyncEnumerableWrapper<TKey>(keys));
        }
    }
}
