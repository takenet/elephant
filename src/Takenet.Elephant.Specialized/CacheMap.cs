using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized
{
    public class CacheMap<TKey, TValue> : CacheStrategy<IMap<TKey, TValue>>, IMap<TKey, TValue>
    {
        public CacheMap(IMap<TKey, TValue> source, IMap<TKey, TValue> cache, TimeSpan synchronizationTimeout)
            : base(source, cache, new OverwriteMapSynchronizer<TKey, TValue>(synchronizationTimeout))
        {
        }

        public CacheMap(IMap<TKey, TValue> source, IMap<TKey, TValue> cache, ISynchronizer<IMap<TKey, TValue>> synchronizer) 
            : base(source, cache, synchronizer)
        {
        }

        public Task<bool> TryAddAsync(TKey key, TValue value, bool overwrite = false)
        {
            return ExecuteWriteFunc(m => m.TryAddAsync(key, value, overwrite));
        }

        public Task<TValue> GetValueOrDefaultAsync(TKey key)
        {
            return ExecuteQueryFunc(m => m.GetValueOrDefaultAsync(key));
        }

        public Task<bool> TryRemoveAsync(TKey key)
        {
            return ExecuteWriteFunc(m => m.TryRemoveAsync(key));
        }

        public Task<bool> ContainsKeyAsync(TKey key)
        {
            return ExecuteQueryFunc(m => m.ContainsKeyAsync(key));
        }
    }
}
