using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized
{
    /// <summary>
    /// Defines a fall back mechanism with a primary and backup maps. 
    /// For write actions, the operation must succeed in both;
    /// For queries, if the action fails in the first, it falls back to the second.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public class BackupMap<TKey, TValue> : BackupStrategy<IMap<TKey, TValue>>, IMap<TKey, TValue>
    {
        public BackupMap(IMap<TKey, TValue> primary, IMap<TKey, TValue> backup, TimeSpan synchronizationTimeout)
            : this(primary, backup, new IntersectionMapSynchronizer<TKey, TValue>(synchronizationTimeout))
        {

        }

        public BackupMap(IMap<TKey, TValue> primary, IMap<TKey, TValue> backup, ISynchronizer<IMap<TKey, TValue>> synchronizer)
            : base(primary, backup, synchronizer)
        {

        }

        public virtual Task<bool> TryAddAsync(TKey key, TValue value, bool overwrite = false)
        {
            return ExecuteWriteFunc(m => m.TryAddAsync(key, value, overwrite));
        }

        public virtual Task<TValue> GetValueOrDefaultAsync(TKey key)
        {
            return ExecuteQueryFunc(m => m.GetValueOrDefaultAsync(key));
        }

        public virtual Task<bool> TryRemoveAsync(TKey key)
        {
            return ExecuteWriteFunc(m => m.TryRemoveAsync(key));
        }

        public virtual Task<bool> ContainsKeyAsync(TKey key)
        {
            return ExecuteQueryFunc(m => m.ContainsKeyAsync(key));
        }
    }
}
