using System;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Specialized.Cache
{
    public class OnDemandCacheMap<TKey, TValue> : OnDemandCacheStrategy<IMap<TKey, TValue>>, IMap<TKey, TValue>, IExpirableKeyMap<TKey, TValue>
    {
        protected readonly TimeSpan CacheExpiration;
        protected readonly TimeSpan CacheFaultTolerance;

        public OnDemandCacheMap(
            IMap<TKey, TValue> source,
            IMap<TKey, TValue> cache,
            TimeSpan cacheExpiration = default,
            TimeSpan cacheFaultTolerance = default)
            : base(source, cache)
        {
            if (cacheExpiration != default && !(cache is IExpirableKeyMap<TKey, TValue>))
            {
                throw new ArgumentException("To enable cache expiration, the cache map should implement IExpirableKeyMap");
            }

            CacheExpiration = cacheExpiration;
            CacheFaultTolerance = cacheFaultTolerance;
        }

        public virtual Task<bool> TryAddAsync(TKey key,
            TValue value,
            bool overwrite = false,
            CancellationToken cancellationToken = default)
            => ExecuteWriteFunc(map => TryAddToMapAsync(key, value, overwrite, map));

        public virtual Task<TValue> GetValueOrDefaultAsync(TKey key, CancellationToken cancellationToken = default) 
            => ExecuteQueryFunc(
                map => map.GetValueOrDefaultAsync(key),
                (result, m) => TryAddToMapAsync(key, result, true, m));

        public virtual Task<bool> TryRemoveAsync(TKey key, CancellationToken cancellationToken = default) 
            => ExecuteWriteFunc(map => map.TryRemoveAsync(key));

        public virtual Task<bool> ContainsKeyAsync(TKey key, CancellationToken cancellationToken = default)
        {
            return ExecuteQueryFunc(
                map => map.ContainsKeyAsync(key),
                async (result, map) =>
                {
                    // If exists in the source and not in the cache
                    if (result)
                    {
                        var value = await Source.GetValueOrDefaultAsync(key).ConfigureAwait(false);
                        if (!IsDefaultValueOfType(value))
                        {
                            return await TryAddToMapAsync(key, value, true, map).ConfigureAwait(false);
                        }
                    }
                    return true;
                });
        }

        private async Task<bool> TryAddToMapAsync(TKey key, TValue value, bool overwrite, IMap<TKey, TValue> map)
        {
            var added = await map.TryAddAsync(key, value, overwrite).ConfigureAwait(false);
            if (added
                && map == Cache
                && CacheExpiration != default(TimeSpan)
                && map is IExpirableKeyMap<TKey, TValue> expirableMap)
            {
                await expirableMap.SetRelativeKeyExpirationAsync(key, CacheExpiration).ConfigureAwait(false);
            }

            return added;
        }

        public async Task SetRelativeKeyExpirationAsync(TKey key, TimeSpan ttl)
        {
            if (!(Source is IExpirableKeyMap<TKey, TValue> expirableSource))
            {
                throw new NotSupportedException("The source map doesn't implement IExpirableKeyMap");
            }

            if (!(Cache is IExpirableKeyMap<TKey, TValue> expirableCache))
            {
                throw new NotSupportedException("The cache map doesn't implement IExpirableKeyMap");
            }

            await expirableSource.SetRelativeKeyExpirationAsync(key, ttl);

            if (ttl < CacheExpiration)
            {
                await expirableCache.SetRelativeKeyExpirationAsync(key, ttl.Add(CacheFaultTolerance));
            }
        }

        public async Task SetAbsoluteKeyExpirationAsync(TKey key, DateTimeOffset expiration)
        {
            if (!(Source is IExpirableKeyMap<TKey, TValue> expirableSource))
            {
                throw new NotSupportedException("The source map doesn't implement IExpirableKeyMap");
            }

            if (!(Cache is IExpirableKeyMap<TKey, TValue> expirableCache))
            {
                throw new NotSupportedException("The cache map doesn't implement IExpirableKeyMap");
            }

            await expirableSource.SetAbsoluteKeyExpirationAsync(key, expiration);

            if (expiration - DateTimeOffset.UtcNow < CacheExpiration)
            {
                await expirableCache.SetAbsoluteKeyExpirationAsync(key, expiration.Add(CacheFaultTolerance));
            }
        }
    }
}
