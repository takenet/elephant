using System;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Specialized.Cache
{
    public class OnDemandCacheMap<TKey, TValue> : OnDemandCacheStrategy<IMap<TKey, TValue>>, IPropertyMap<TKey, TValue>, IExpirableKeyMap<TKey, TValue>
    {
        protected readonly TimeSpan CacheExpiration;
        protected readonly TimeSpan CacheFaultTolerance;
        private readonly bool _implementsPropertyMap;

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
            _implementsPropertyMap = Source is IPropertyMap<TKey, TValue> && 
                                     Cache is IPropertyMap<TKey, TValue>;
        }

        public virtual Task<bool> TryAddAsync(TKey key,
            TValue value,
            bool overwrite = false,
            CancellationToken cancellationToken = default) => 
            ExecuteWriteFunc(map => TryAddWithExpirationAsync(key, value, overwrite, map, cancellationToken));

        public virtual Task<TValue> GetValueOrDefaultAsync(TKey key, CancellationToken cancellationToken = default) => 
            ExecuteQueryFunc(
                map => map.GetValueOrDefaultAsync(key, cancellationToken),
                (result, m) => TryAddWithExpirationAsync(key, result, true, m, cancellationToken));

        public virtual Task<bool> TryRemoveAsync(TKey key, CancellationToken cancellationToken = default) => 
            ExecuteWriteFunc(map => map.TryRemoveAsync(key, cancellationToken));

        public virtual Task<bool> ContainsKeyAsync(TKey key, CancellationToken cancellationToken = default)
        {
            return ExecuteQueryFunc(
                map => map.ContainsKeyAsync(key),
                async (result, map) =>
                {
                    // If exists in the source and not in the cache
                    if (result)
                    {
                        var value = await Source.GetValueOrDefaultAsync(key, cancellationToken).ConfigureAwait(false);
                        if (!IsDefaultValueOfType(value))
                        {
                            return await TryAddWithExpirationAsync(key, value, true, map, cancellationToken).ConfigureAwait(false);
                        }
                    }
                    return true;
                });
        }

        public virtual Task SetPropertyValueAsync<TProperty>(TKey key, string propertyName, TProperty propertyValue,
            CancellationToken cancellationToken = default)
        {
            if (!_implementsPropertyMap)
            {
                return Task.FromException(
                    new NotSupportedException("Either the source or cache map doesn't implement IPropertyMap"));
            }
            
            return ExecuteWriteFunc(map => ((IPropertyMap<TKey, TValue>)map).SetPropertyValueAsync(key, propertyName, propertyValue, cancellationToken));
        }

        public virtual Task<TProperty> GetPropertyValueOrDefaultAsync<TProperty>(TKey key, string propertyName,
            CancellationToken cancellationToken = default)
        {
            if (!_implementsPropertyMap)
            {
                return Task.FromException<TProperty>(
                    new NotSupportedException("Either the source or cache map doesn't implement IPropertyMap"));
            }
            
            return ExecuteQueryFunc(map =>
                    ((IPropertyMap<TKey, TValue>)map).GetPropertyValueOrDefaultAsync<TProperty>(key, propertyName, cancellationToken),
                (propertyValue, map) =>
                    ((IPropertyMap<TKey, TValue>)map).SetPropertyValueAsync(key, propertyName, propertyValue, cancellationToken));
        }

        public virtual Task MergeAsync(TKey key, TValue value, CancellationToken cancellationToken = default)
        {
            if (!_implementsPropertyMap)
            {
                return Task.FromException(
                    new NotSupportedException("Either the source or cache map doesn't implement IPropertyMap"));
            }
            
            return ExecuteWriteFunc(map => ((IPropertyMap<TKey, TValue>)map).MergeAsync(key, value, cancellationToken));
        }

        public virtual async Task<bool> SetRelativeKeyExpirationAsync(TKey key, TimeSpan ttl)
        {
            if (!(Source is IExpirableKeyMap<TKey, TValue> expirableSource))
            {
                throw new NotSupportedException("The source map doesn't implement IExpirableKeyMap");
            }

            if (!(Cache is IExpirableKeyMap<TKey, TValue> expirableCache))
            {
                throw new NotSupportedException("The cache map doesn't implement IExpirableKeyMap");
            }

            var success = await expirableSource.SetRelativeKeyExpirationAsync(key, ttl);
            if (success &&
                ttl < CacheExpiration)
            {
                success = await expirableCache.SetRelativeKeyExpirationAsync(key, ttl.Add(CacheFaultTolerance));
            }

            return success;
        }

        public virtual async Task<bool> SetAbsoluteKeyExpirationAsync(TKey key, DateTimeOffset expiration)
        {
            if (!(Source is IExpirableKeyMap<TKey, TValue> expirableSource))
            {
                throw new NotSupportedException("The source map doesn't implement IExpirableKeyMap");
            }

            if (!(Cache is IExpirableKeyMap<TKey, TValue> expirableCache))
            {
                throw new NotSupportedException("The cache map doesn't implement IExpirableKeyMap");
            }

            var success = await expirableSource.SetAbsoluteKeyExpirationAsync(key, expiration);
            if (success &&
                expiration - DateTimeOffset.UtcNow < CacheExpiration)
            {
                success = await expirableCache.SetAbsoluteKeyExpirationAsync(key, expiration.Add(CacheFaultTolerance));
            }

            return success;
        }
        
        protected async Task<bool> TryAddWithExpirationAsync(TKey key, TValue value, bool overwrite, IMap<TKey, TValue> map, CancellationToken cancellationToken)
        {
            var added = await map.TryAddAsync(key, value, overwrite, cancellationToken).ConfigureAwait(false);
            if (added && 
                map == Cache && 
                CacheExpiration != default && 
                map is IExpirableKeyMap<TKey, TValue> expirableMap)
            {
                await expirableMap.SetRelativeKeyExpirationAsync(key, CacheExpiration).ConfigureAwait(false);
            }

            return added;
        }
    }
}
