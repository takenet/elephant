using System;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Specialized.Synchronization;

namespace Take.Elephant.Specialized.Cache
{
    public class CacheMap<TKey, TValue> : CacheStrategy<IMap<TKey, TValue>>, IMap<TKey, TValue>
    {
        public CacheMap(IMap<TKey, TValue> source, IMap<TKey, TValue> cache, TimeSpan synchronizationTimeout, TimeSpan cacheExpiration = default(TimeSpan))
            : base(source, cache, new OverwriteMapSynchronizer<TKey, TValue>(synchronizationTimeout), cacheExpiration)
        {
        }

        protected CacheMap(IMap<TKey, TValue> source, IMap<TKey, TValue> cache, ISynchronizer<IMap<TKey, TValue>> synchronizer, TimeSpan cacheExpiration = default(TimeSpan)) 
            : base(source, cache, synchronizer, cacheExpiration)
        {
        }

        public virtual Task<bool> TryAddAsync(TKey key,
            TValue value,
            bool overwrite = false,
            CancellationToken cancellationToken = default)
        {
            return ExecuteWriteFunc(m => m.TryAddAsync(key, value, overwrite));
        }

        public virtual Task<TValue> GetValueOrDefaultAsync(TKey key, CancellationToken cancellationToken = default)
        {
            return ExecuteQueryFunc(m => m.GetValueOrDefaultAsync(key));
        }

        public virtual Task<bool> TryRemoveAsync(TKey key, CancellationToken cancellationToken = default)
        {
            return ExecuteWriteFunc(m => m.TryRemoveAsync(key));
        }

        public virtual Task<bool> ContainsKeyAsync(TKey key, CancellationToken cancellationToken = default)
        {
            return ExecuteQueryFunc(m => m.ContainsKeyAsync(key));
        }
    }
}
