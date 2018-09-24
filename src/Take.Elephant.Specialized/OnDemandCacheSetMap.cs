using System;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Memory;

namespace Take.Elephant.Specialized
{
    public class OnDemandCacheSetMap<TKey, TValue> : OnDemandCacheMap<TKey, ISet<TValue>>, ISetMap<TKey, TValue>
    {
        public OnDemandCacheSetMap(ISetMap<TKey, TValue> source, ISetMap<TKey, TValue> cache, TimeSpan cacheExpiration = default(TimeSpan))
            : base(source, cache, cacheExpiration)
        {
        }

        public override async Task<ISet<TValue>> GetValueOrDefaultAsync(TKey key,
            CancellationToken cancellationToken = default)
        {
            var cacheValue = await Cache.GetValueOrDefaultAsync(key, cancellationToken).ConfigureAwait(false);
            if (cacheValue != null)
            {
                // The source should be lazy to avoid queries on the source for checking availability
                return new OnDemandCacheSet<TValue>(
                    new LazySet<TValue>(() => ((ISetMap<TKey, TValue>)Source).GetValueOrEmptyAsync(key, cancellationToken)),
                    cacheValue);
            }

            var sourceValue = await Source.GetValueOrDefaultAsync(key, cancellationToken).ConfigureAwait(false);
            if (sourceValue == null) return null;

            // Passes an empty set for futher synchronization
            cacheValue = await ((ISetMap<TKey, TValue>)Cache).GetValueOrEmptyAsync(key, cancellationToken);
            return new OnDemandCacheSet<TValue>(sourceValue, GetKeyExpirationCacheSet(key, cacheValue));
        }

        public virtual async Task<ISet<TValue>> GetValueOrEmptyAsync(TKey key, CancellationToken cancellationToken = default)
        {
            var cacheValue = await ((ISetMap<TKey, TValue>)Cache).GetValueOrEmptyAsync(key, cancellationToken).ConfigureAwait(false);
            if (await cacheValue.GetLengthAsync(cancellationToken).ConfigureAwait(false) > 0)
            {
                // The source should be lazy to avoid queries on the source for checking availability
                return new OnDemandCacheSet<TValue>(
                    new LazySet<TValue>(() => ((ISetMap<TKey, TValue>)Source).GetValueOrEmptyAsync(key, cancellationToken)),
                    cacheValue);
            }

            var sourceValue = await ((ISetMap<TKey, TValue>)Source).GetValueOrEmptyAsync(key, cancellationToken).ConfigureAwait(false);

            // Passes an empty set for futher synchronization            
            return new OnDemandCacheSet<TValue>(sourceValue, GetKeyExpirationCacheSet(key, cacheValue));
        }

        private ISet<TValue> GetKeyExpirationCacheSet(TKey key, ISet<TValue> cacheSet)
        {
            // Provides a set that calls a function to expires the key when a value is added
            if (CacheExpiration != default(TimeSpan) && Cache is IExpirableKeyMap<TKey, ISet<TValue>> expirableMap)
            {
                return new TriggeredSet<TValue>(cacheSet, i => expirableMap.SetRelativeKeyExpirationAsync(key, CacheExpiration));                
            }
            
            return cacheSet;
        }

        private sealed class TriggeredSet<T> : ISet<T>
        {
            private readonly ISet<T> _set;
            private readonly Func<T, Task> _addTrigger;

            public TriggeredSet(ISet<T> set, Func<T, Task> addTrigger)
            {
                _set = set;
                _addTrigger = addTrigger;
            }

            public async Task AddAsync(T value, CancellationToken cancellationToken = default)
            {
                await _set.AddAsync(value, cancellationToken).ConfigureAwait(false);
                await _addTrigger(value).ConfigureAwait(false);
            }

            public Task<IAsyncEnumerable<T>> AsEnumerableAsync(CancellationToken cancellationToken = default) => _set.AsEnumerableAsync();

            public Task<bool> ContainsAsync(T value, CancellationToken cancellationToken = default) => _set.ContainsAsync(value);

            public Task<long> GetLengthAsync(CancellationToken cancellationToken = default) => _set.GetLengthAsync();

            public Task<bool> TryRemoveAsync(T value, CancellationToken cancellationToken = default) => _set.TryRemoveAsync(value);
        }

        private sealed class LazySet<T> : ISet<T>
        {
            private readonly Lazy<Task<ISet<T>>> _lazySet;

            public LazySet(Func<Task<ISet<T>>> factory)
            {
                _lazySet = new Lazy<Task<ISet<T>>>(factory);
            }

            public async Task AddAsync(T value, CancellationToken cancellationToken = default)
            {
                var set = await GetSetAsync().ConfigureAwait(false);
                await set.AddAsync(value, cancellationToken).ConfigureAwait(false);
            }

            public async Task<IAsyncEnumerable<T>> AsEnumerableAsync(CancellationToken cancellationToken = default)
            {
                var set = await GetSetAsync().ConfigureAwait(false);
                return await set.AsEnumerableAsync(cancellationToken).ConfigureAwait(false);
            }

            public async Task<bool> ContainsAsync(T value, CancellationToken cancellationToken = default)
            {
                var set = await GetSetAsync().ConfigureAwait(false);
                return await set.ContainsAsync(value, cancellationToken).ConfigureAwait(false);
            }

            public async Task<long> GetLengthAsync(CancellationToken cancellationToken = default)
            {
                var set = await GetSetAsync().ConfigureAwait(false);
                return await set.GetLengthAsync(cancellationToken).ConfigureAwait(false);
            }

            public async Task<bool> TryRemoveAsync(T value, CancellationToken cancellationToken = default)
            {
                var set = await GetSetAsync().ConfigureAwait(false);
                return await set.TryRemoveAsync(value, cancellationToken).ConfigureAwait(false);
            }

            private async Task<ISet<T>> GetSetAsync()
            {
                var set = await _lazySet.Value.ConfigureAwait(false);
                if (set == null) throw new InvalidOperationException("The set factory returned null");
                return set;
            }
        }
    }
}
