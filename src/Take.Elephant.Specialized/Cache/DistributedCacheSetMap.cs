using System;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Memory;

namespace Take.Elephant.Specialized.Cache
{
    public class DistributedCacheSetMap<TKey, TValue> : ISetMap<TKey, TValue>
    {
        private readonly DistributedCacheMap<TKey, ISet<TValue>> _map;
        public DistributedCacheSetMap(
            ISetMap<TKey, TValue> source, 
            IBus<string, SynchronizationEvent<TKey>> synchronizationBus, 
            string synchronizationChannel, 
            TimeSpan cacheExpiration = default, 
            TimeSpan cacheFaultTolerance = default)
        {
            _map = new DistributedCacheMap<TKey, ISet<TValue>>(
                () => new SetMap<TKey, TValue>(), 
                (m, c, e, t) => new OnDemandCacheSetMap<TKey, TValue>((ISetMap<TKey, TValue>)m, (SetMap<TKey, TValue>)c, e, t),
                source, 
                synchronizationBus, 
                synchronizationChannel, 
                cacheExpiration, 
                cacheFaultTolerance);
        }
        
        public virtual Task<bool> TryAddAsync(TKey key, ISet<TValue> value, bool overwrite = false, CancellationToken cancellationToken = default) => 
            _map.TryAddAsync(key, value, overwrite, cancellationToken);

        public virtual Task<bool> TryRemoveAsync(TKey key, CancellationToken cancellationToken = default) => 
            _map.TryRemoveAsync(key, cancellationToken);

        public virtual Task<bool> ContainsKeyAsync(TKey key, CancellationToken cancellationToken = default) => 
            _map.ContainsKeyAsync(key, cancellationToken);

        public virtual async Task<ISet<TValue>> GetValueOrDefaultAsync(TKey key, CancellationToken cancellationToken = default)
        {
            var set = await _map.GetValueOrDefaultAsync(key, cancellationToken);
            if (set != null)
            {
                
            }

            return set;
        }

        public async Task<ISet<TValue>> GetValueOrEmptyAsync(TKey key, CancellationToken cancellationToken = default)
        {
            var set = await ((OnDemandCacheSetMap<TKey, TValue>)_map.OnDemandCacheMap).GetValueOrEmptyAsync(key, cancellationToken);

            return set;
        }
    }
}