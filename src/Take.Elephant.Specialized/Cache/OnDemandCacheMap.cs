using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Take.Elephant.Specialized.Cache
{
    public class OnDemandCacheMap<TKey, TValue> : OnDemandCacheBase<IMap<TKey, TValue>>, IPropertyMap<TKey, TValue>, IExpirableKeyMap<TKey, TValue>
    {
        private readonly bool _implementsPropertyMap;

        public OnDemandCacheMap(
            IMap<TKey, TValue> source,
            IMap<TKey, TValue> cache,
            TimeSpan cacheExpiration = default,
            TimeSpan cacheFaultTolerance = default,
            bool cacheMissingValues = false)
            : this(source, cache, new CacheOptions { CacheExpiration = cacheExpiration, CacheFaultTolerance = cacheFaultTolerance, CacheMissingValues = cacheMissingValues }, new TraceLogger())
        {
        }

        public OnDemandCacheMap(
            IMap<TKey, TValue> source,
            IMap<TKey, TValue> cache,
            CacheOptions cacheOptions,
            ILogger logger)
            : base(source, cache, cacheOptions, logger)
        {
            if (cacheOptions.CacheExpiration != default && !(cache is IExpirableKeyMap<TKey, TValue>))
            {
                throw new ArgumentException("To enable cache expiration, the cache map should implement IExpirableKeyMap");
            }
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
                        if (!IsDefaultValueOfType(value) || (value != null && CacheOptions.CacheMissingValues))
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
            var expirableOnDemand = VerifySourceCacheExpirableType();

            var success = await expirableOnDemand.expirableSource.SetRelativeKeyExpirationAsync(key, ttl);
            if (success &&
                ttl < CacheOptions.CacheExpiration)
            {
                success = await expirableOnDemand.expirableCache.SetRelativeKeyExpirationAsync(key, ttl.Add(CacheOptions.CacheFaultTolerance));
            }

            return success;
        }

        public virtual async Task<bool> SetAbsoluteKeyExpirationAsync(TKey key, DateTimeOffset expiration)
        {
            var expirableOnDemand = VerifySourceCacheExpirableType();

            var success = await expirableOnDemand.expirableSource.SetAbsoluteKeyExpirationAsync(key, expiration);
            if (success &&
                expiration - DateTimeOffset.UtcNow < CacheOptions.CacheExpiration)
            {
                success = await expirableOnDemand.expirableCache.SetAbsoluteKeyExpirationAsync(key, expiration.Add(CacheOptions.CacheFaultTolerance));
            }

            return success;
        }

        public virtual async Task<bool> RemoveExpirationAsync(TKey key)
        {
            var expirableOnDemand = VerifySourceCacheExpirableType();

            var success = await expirableOnDemand.expirableSource.RemoveExpirationAsync(key);
            if (success)
            {
                success = await expirableOnDemand.expirableCache.RemoveExpirationAsync(key);
            }

            return success;
        }

        private (IExpirableKeyMap<TKey, TValue> expirableSource, IExpirableKeyMap<TKey, TValue> expirableCache) VerifySourceCacheExpirableType()
        {
            if (!(Source is IExpirableKeyMap<TKey, TValue> expirableSource))
            {
                throw new NotSupportedException("The source map doesn't implement IExpirableKeyMap");
            }

            if (!(Cache is IExpirableKeyMap<TKey, TValue> expirableCache))
            {
                throw new NotSupportedException("The cache map doesn't implement IExpirableKeyMap");
            }

            return (expirableSource, expirableCache);
        }

        protected async Task<bool> TryAddWithExpirationAsync(TKey key, TValue value, bool overwrite, IMap<TKey, TValue> map, CancellationToken cancellationToken)
        {
            var added = await map.TryAddAsync(key, value, overwrite, cancellationToken).ConfigureAwait(false);
            if (added &&
                map == Cache &&
                CacheOptions.CacheExpiration != default &&
                map is IExpirableKeyMap<TKey, TValue> expirableMap)
            {
                await expirableMap.SetRelativeKeyExpirationAsync(key, CacheOptions.CacheExpiration).ConfigureAwait(false);
            }

            return added;
        }
    }
}
