using System;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Memory;
using Take.Elephant.Specialized.NotifyWrite;

namespace Take.Elephant.Specialized.Cache
{
    public class DistributedCacheSetMap<TKey, TValue> : ISetMap<TKey, TValue>, IAsyncDisposable, IDisposable
    {
        private readonly ISetMap<TKey, TValue> _underlyingSetMap;
        private readonly DistributedCacheStrategy<TKey, ISet<TValue>> _strategy;

        public DistributedCacheSetMap(
            ISetMap<TKey, TValue> source, 
            IBus<string, SynchronizationEvent<TKey>> synchronizationBus, 
            string synchronizationChannel, 
            TimeSpan cacheExpiration = default, 
            TimeSpan cacheFaultTolerance = default)
        {
            var memoryCache = new SetMap<TKey, TValue>();
            var onDemandCacheMap = new OnDemandCacheSetMap<TKey, TValue>(source, memoryCache, cacheExpiration, cacheFaultTolerance);
            _strategy = new DistributedCacheStrategy<TKey, ISet<TValue>>(memoryCache, synchronizationBus, synchronizationChannel);
            _underlyingSetMap = new NotifyWriteSetMap<TKey, TValue>(onDemandCacheMap, _strategy.PublishEventAsync);
        }

        public Task<bool> TryAddAsync(TKey key, ISet<TValue> value, bool overwrite = false, CancellationToken cancellationToken = default) => 
            _underlyingSetMap.TryAddAsync(key, value, overwrite, cancellationToken);

        public Task<ISet<TValue>> GetValueOrDefaultAsync(TKey key, CancellationToken cancellationToken = default) => 
            _underlyingSetMap.GetValueOrDefaultAsync(key, cancellationToken);

        public Task<bool> TryRemoveAsync(TKey key, CancellationToken cancellationToken = default) => 
            _underlyingSetMap.TryRemoveAsync(key, cancellationToken);

        public Task<bool> ContainsKeyAsync(TKey key, CancellationToken cancellationToken = default) => 
            _underlyingSetMap.ContainsKeyAsync(key, cancellationToken);

        public Task<ISet<TValue>> GetValueOrEmptyAsync(TKey key, CancellationToken cancellationToken = default) => 
            _underlyingSetMap.GetValueOrEmptyAsync(key, cancellationToken);

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