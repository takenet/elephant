using System;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Memory;
using Take.Elephant.Specialized.NotifyWrite;

namespace Take.Elephant.Specialized.Cache
{
    /// <summary>
    /// Implements a <see cref="IMap{TKey,TValue}"/> that cache the values on demand in memory, using a bus to invalidate the cache between instances. 
    /// </summary>
    public class DistributedCacheMap<TKey, TValue> : IPropertyMap<TKey, TValue>, IAsyncDisposable, IDisposable
    {
        private readonly IPropertyMap<TKey, TValue> _underlyingMap;
        private readonly DistributedCacheStrategy<TKey, TValue> _strategy;

        public DistributedCacheMap(
            IMap<TKey, TValue> source,
            IBus<string, SynchronizationEvent<TKey>> synchronizationBus,
            string synchronizationChannel,
            TimeSpan cacheExpiration = default,
            TimeSpan cacheFaultTolerance = default)
        {
            var memoryCache = new Map<TKey, TValue>();
            var onDemandCacheMap = new OnDemandCacheMap<TKey, TValue>(source, memoryCache, cacheExpiration, cacheFaultTolerance);
            _strategy = new DistributedCacheStrategy<TKey, TValue>(memoryCache, synchronizationBus, synchronizationChannel);
            _underlyingMap = new NotifyWriteMap<TKey, TValue>(onDemandCacheMap, _strategy.PublishEventAsync);
        }
        
        public virtual Task<bool> TryAddAsync(TKey key, TValue value, bool overwrite = false, CancellationToken cancellationToken = default) => 
            _underlyingMap.TryAddAsync(key, value, overwrite, cancellationToken);

        public virtual Task<TValue> GetValueOrDefaultAsync(TKey key, CancellationToken cancellationToken = default) => 
            _underlyingMap.GetValueOrDefaultAsync(key, cancellationToken);

        public virtual Task<bool> TryRemoveAsync(TKey key, CancellationToken cancellationToken = default) => 
            _underlyingMap.TryRemoveAsync(key, cancellationToken);

        public virtual Task<bool> ContainsKeyAsync(TKey key, CancellationToken cancellationToken = default) => 
            _underlyingMap.ContainsKeyAsync(key, cancellationToken);

        public virtual Task SetPropertyValueAsync<TProperty>(TKey key, string propertyName, TProperty propertyValue, CancellationToken cancellationToken = default) =>
            _underlyingMap.SetPropertyValueAsync(key, propertyName, propertyValue, cancellationToken);

        public virtual Task<TProperty> GetPropertyValueOrDefaultAsync<TProperty>(TKey key, string propertyName,
            CancellationToken cancellationToken = default) =>
            _underlyingMap.GetPropertyValueOrDefaultAsync<TProperty>(key, propertyName, cancellationToken);

        public virtual Task MergeAsync(TKey key, TValue value, CancellationToken cancellationToken = default) => 
            _underlyingMap.MergeAsync(key, value, cancellationToken);

        public virtual ValueTask DisposeAsync() => _strategy.DisposeAsync();

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                DisposeAsync().GetAwaiter().GetResult();
            }
        }
    }
}