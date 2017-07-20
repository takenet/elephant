using System;
using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized
{
    public class OnDemandCacheMap<TKey, TValue> : OnDemandCacheStrategy<IMap<TKey, TValue>>, IMap<TKey, TValue>
    {
        protected readonly TimeSpan CacheExpiration;

        public OnDemandCacheMap(IMap<TKey, TValue> source, IMap<TKey, TValue> cache, TimeSpan cacheExpiration = default(TimeSpan))
            : base(source, cache)
        {
            if (cacheExpiration != default(TimeSpan)
                && !(cache is IExpirableKeyMap<TKey, TValue>))
            {
                throw new ArgumentException("To enable cache expiration, the cache map should implement IExpirableKeyMap");
            }

            CacheExpiration = cacheExpiration;
        }

        public virtual Task<bool> TryAddAsync(TKey key, TValue value, bool overwrite = false)
            => ExecuteWriteFunc(map => map.TryAddAsync(key, value, overwrite));

        public virtual Task<TValue> GetValueOrDefaultAsync(TKey key) 
            => ExecuteQueryFunc(
                map => map.GetValueOrDefaultAsync(key),
                (result, m) => AddToMapAsync(key, result, m));

        public virtual Task<bool> TryRemoveAsync(TKey key) 
            => ExecuteWriteFunc(map => map.TryRemoveAsync(key));

        public virtual Task<bool> ContainsKeyAsync(TKey key)
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
                            return await AddToMapAsync(key, value, map).ConfigureAwait(false);
                        }
                    }
                    return true;
                });
        }

        private async Task<bool> AddToMapAsync(TKey key, TValue value, IMap<TKey, TValue> map)
        {
            var added = await map.TryAddAsync(key, value, true).ConfigureAwait(false);
            if (added
                && CacheExpiration != default(TimeSpan)
                && map is IExpirableKeyMap<TKey, TValue> expirableMap)
            {
                await expirableMap.SetRelativeKeyExpirationAsync(key, CacheExpiration).ConfigureAwait(false);
            }

            return added;
        }
    }
}
