using System;
using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized
{
    public class OnDemandCacheMap<TKey, TValue> : OnDemandCacheStrategy<IMap<TKey, TValue>>, IMap<TKey, TValue>
    {
        private readonly TimeSpan _cacheExpiration;

        public OnDemandCacheMap(IMap<TKey, TValue> source, IMap<TKey, TValue> cache, TimeSpan cacheExpiration = default(TimeSpan))
            : base(source, cache)
        {
            _cacheExpiration = cacheExpiration;
        }

        public virtual Task<bool> TryAddAsync(TKey key, TValue value, bool overwrite = false)
            => ExecuteWriteFunc(m => m.TryAddAsync(key, value, overwrite));

        public virtual Task<TValue> GetValueOrDefaultAsync(TKey key) 
            => ExecuteQueryFunc(
                m => m.GetValueOrDefaultAsync(key),
                (v, m) => AddToMapAsync(key, v, m));

        public virtual Task<bool> TryRemoveAsync(TKey key) 
            => ExecuteWriteFunc(m => m.TryRemoveAsync(key));

        public virtual Task<bool> ContainsKeyAsync(TKey key)
        {
            return ExecuteQueryFunc(
                m => m.ContainsKeyAsync(key),
                async (v, m) =>
                {
                    // If exists in the source and not in the cache
                    if (v)
                    {
                        var value = await Source.GetValueOrDefaultAsync(key).ConfigureAwait(false);
                        if (!IsDefaultValueOfType(value))
                        {
                            return await AddToMapAsync(key, value, m);
                        }
                    }
                    return true;
                });
        }

        private async Task<bool> AddToMapAsync(TKey key, TValue value, IMap<TKey, TValue> map)
        {
            var added = await map.TryAddAsync(key, value, true).ConfigureAwait(false);
            if (added
                && map is IExpirableKeyMap<TKey, TValue> expirableMap)
            {
                await expirableMap.SetRelativeKeyExpirationAsync(key, _cacheExpiration).ConfigureAwait(false);
            }

            return added;
        }
    }
}
