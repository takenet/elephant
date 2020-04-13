using System;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Memory;

namespace Take.Elephant.Specialized.Cache
{
    /// <summary>
    /// Implements a <see cref="IMap{TKey,TValue}"/> that cache the values on demand in memory, using a bus to invalidate the cache between instances. 
    /// </summary>
    public class DistributedCacheMap<TKey, TValue> : IPropertyMap<TKey, TValue>
    {
        private readonly IBus<string, SynchronizationEvent<TKey>> _synchronizationBus;
        private readonly string _synchronizationChannel;
        private readonly Guid _instance;
        private readonly Task _subscriptionTask;
        
        internal readonly OnDemandCacheMap<TKey, TValue> OnDemandCacheMap;
        private readonly Map<TKey, TValue> _memoryCache;

        internal delegate Map<TKey, TValue> CacheFactory();
        internal delegate OnDemandCacheMap<TKey, TValue> MapFactory(IMap<TKey, TValue> source, Map<TKey, TValue> cache, TimeSpan cacheExpiration, TimeSpan cacheFaultTolerance);
        
        public DistributedCacheMap(
            IMap<TKey, TValue> source,
            IBus<string, SynchronizationEvent<TKey>> synchronizationBus,
            string synchronizationChannel,
            TimeSpan cacheExpiration = default,
            TimeSpan cacheFaultTolerance = default)
            : this(
                () => new Map<TKey, TValue>(),
                (m, c, e, t) => new OnDemandCacheMap<TKey, TValue>(m, c, e, t),
                source,
                synchronizationBus,
                synchronizationChannel,
                cacheExpiration,
                cacheFaultTolerance)
        {

        }
        
        internal DistributedCacheMap(
            CacheFactory cacheFactory,
            MapFactory mapFactory,
            IMap<TKey, TValue> source,
            IBus<string, SynchronizationEvent<TKey>> synchronizationBus,
            string synchronizationChannel,
            TimeSpan cacheExpiration = default,
            TimeSpan cacheFaultTolerance = default)
        {
            _memoryCache = cacheFactory();
            OnDemandCacheMap = mapFactory(source, _memoryCache, cacheExpiration, cacheFaultTolerance);
            _synchronizationBus = synchronizationBus ?? throw new ArgumentNullException(nameof(synchronizationBus));
            _synchronizationChannel = synchronizationChannel ?? throw new ArgumentNullException(nameof(synchronizationChannel));
            _instance = Guid.NewGuid();
            _subscriptionTask = _synchronizationBus.SubscribeAsync(_synchronizationChannel, HandleEventAsync, CancellationToken.None);
        }

        public virtual async Task<bool> TryAddAsync(TKey key, TValue value, bool overwrite = false, CancellationToken cancellationToken = new CancellationToken())
        {
            await EnsureSubscribedAsync(cancellationToken);
            
            if (await OnDemandCacheMap.TryAddAsync(key, value, overwrite, cancellationToken))
            {
                await PublishEventAsync(key, cancellationToken);
                return true;
            }

            return false;
        }

        public virtual Task<TValue> GetValueOrDefaultAsync(TKey key, CancellationToken cancellationToken = default) => OnDemandCacheMap.GetValueOrDefaultAsync(key, cancellationToken);

        public virtual async Task<bool> TryRemoveAsync(TKey key, CancellationToken cancellationToken = new CancellationToken())
        {
            await EnsureSubscribedAsync(cancellationToken);

            if (await OnDemandCacheMap.TryRemoveAsync(key, cancellationToken))
            {
                await PublishEventAsync(key, cancellationToken);
                return true;
            }

            return false;
        }

        public virtual Task<bool> ContainsKeyAsync(TKey key, CancellationToken cancellationToken = default) => OnDemandCacheMap.ContainsKeyAsync(key, cancellationToken);

        public virtual Task<TProperty> GetPropertyValueOrDefaultAsync<TProperty>(TKey key, string propertyName,
            CancellationToken cancellationToken = default) => OnDemandCacheMap.GetPropertyValueOrDefaultAsync<TProperty>(key, propertyName, cancellationToken);

        public virtual async Task MergeAsync(TKey key, TValue value, CancellationToken cancellationToken = default)
        {
            await EnsureSubscribedAsync(cancellationToken);
            
            await OnDemandCacheMap.MergeAsync(key, value, cancellationToken);
            await PublishEventAsync(key, cancellationToken);
        }

        public virtual async Task SetPropertyValueAsync<TProperty>(TKey key, string propertyName,
            TProperty propertyValue, CancellationToken cancellationToken = default)
        {
            await EnsureSubscribedAsync(cancellationToken);
            
            await OnDemandCacheMap.SetPropertyValueAsync(key, propertyName, propertyValue, cancellationToken);
            await PublishEventAsync(key, cancellationToken);
        }
        
        private async Task EnsureSubscribedAsync(CancellationToken cancellationToken = default)
        {
            if (!_subscriptionTask.IsCompleted)
            {
                var tcs = new TaskCompletionSource<object>();
                await using var _ = cancellationToken.Register(() => tcs.TrySetCanceled(cancellationToken));
                await Task.WhenAny(_subscriptionTask, tcs.Task);
                cancellationToken.ThrowIfCancellationRequested();
            }
        }
        
        private async Task HandleEventAsync(string synchronizationChannel, SynchronizationEvent<TKey> @event, CancellationToken cancellationToken)
        {
            // Ignore events generated by the current instance.
            if (@event.Instance == _instance) return;

            // Remove from the cache either if it is a new key (which will force the value to be reloaded from the source) or if it was removed. 
            await _memoryCache.TryRemoveAsync(@event.Key, cancellationToken);
        }
        
        private async Task PublishEventAsync(TKey key, CancellationToken cancellationToken = default)
        {
            await _synchronizationBus.PublishAsync(
                _synchronizationChannel,
                new SynchronizationEvent<TKey>()
                {
                    Key = key,
                    Instance = _instance
                },
                cancellationToken);
        }
    }

    public sealed class WriteHandlerMap<TKey, TValue> : IPropertyMap<TKey, TValue>
    {
        private readonly IPropertyMap<TKey, TValue> _map;
        private readonly Func<TKey, Task> _writeHandler;

        public WriteHandlerMap(IPropertyMap<TKey, TValue> map, Func<TKey, Task> writeHandler)
        {
            _map = map;
            _writeHandler = writeHandler;
        }

        public Task<bool> TryAddAsync(TKey key, TValue value, bool overwrite = false, CancellationToken cancellationToken = default)
        {
            return _map.TryAddAsync(key, value, overwrite, cancellationToken);
        }

        public Task<TValue> GetValueOrDefaultAsync(TKey key, CancellationToken cancellationToken = default)
        {
            return _map.GetValueOrDefaultAsync(key, cancellationToken);
        }

        public Task<bool> TryRemoveAsync(TKey key, CancellationToken cancellationToken = default)
        {
            return _map.TryRemoveAsync(key, cancellationToken);
        }

        public Task<bool> ContainsKeyAsync(TKey key, CancellationToken cancellationToken = default)
        {
            return _map.ContainsKeyAsync(key, cancellationToken);
        }

        public Task SetPropertyValueAsync<TProperty>(TKey key, string propertyName, TProperty propertyValue,
            CancellationToken cancellationToken = default)
        {
            return _map.SetPropertyValueAsync(key, propertyName, propertyValue, cancellationToken);
        }

        public Task<TProperty> GetPropertyValueOrDefaultAsync<TProperty>(TKey key, string propertyName,
            CancellationToken cancellationToken = default)
        {
            return _map.GetPropertyValueOrDefaultAsync<TProperty>(key, propertyName, cancellationToken);
        }

        public Task MergeAsync(TKey key, TValue value, CancellationToken cancellationToken = default)
        {
            return _map.MergeAsync(key, value, cancellationToken);
        }
    }
}