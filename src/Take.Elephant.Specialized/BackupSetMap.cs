using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Take.Elephant.Specialized
{    
    /// <summary>
    /// Defines a fall back mechanism with a primary and backup maps. 
    /// For write actions, the operation must succeed in both;
    /// For queries, if the action fails in the first, it falls back to the second.
    /// </summary>    
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public class BackupSetMap<TKey, TValue> : BackupMap<TKey, ISet<TValue>>, ISetMap<TKey, TValue>
    {
        public BackupSetMap(ISetMap<TKey, TValue> primary, ISetMap<TKey, TValue> backup, TimeSpan synchronizationTimeout)
            : base(primary, backup, synchronizationTimeout)
        {

        }

        public BackupSetMap(ISetMap<TKey, TValue> primary, ISetMap<TKey, TValue> backup, ISynchronizer<IMap<TKey, ISet<TValue>>> synchronizer)
            : base(primary, backup, synchronizer)
        {
        }

        public virtual Task<ISet<TValue>> GetValueOrEmptyAsync(TKey key)
        {
            return ExecuteQueryFunc(m => ((ISetMap<TKey, TValue>)m).GetValueOrEmptyAsync(key));            
        }
    }
}
