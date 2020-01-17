using System;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Specialized.Synchronization;

namespace Take.Elephant.Specialized.Replication
{
    /// <summary>
    /// Implements a replication mechanism with a master and slave maps. 
    /// For write actions, the operation must succeed in both;
    /// For queries, if the action fails in the first, it falls back to the second.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public class ReplicationMap<TKey, TValue> : ReplicationStrategy<IMap<TKey, TValue>>, IMap<TKey, TValue>
    {
        public ReplicationMap(IMap<TKey, TValue> master, IMap<TKey, TValue> slave, TimeSpan synchronizationTimeout)
            : this(master, slave, new DifferentialMapSynchronizer<TKey, TValue>(synchronizationTimeout))
        {
            if (!(slave is IKeysMap<TKey, TValue>)) throw new ArgumentException("The slave map must implement IKeysMap to allow synchronization");            
        }

        public ReplicationMap(IMap<TKey, TValue> master, IMap<TKey, TValue> slave, ISynchronizer<IMap<TKey, TValue>> synchronizer)
            : base(master, slave, synchronizer)
        {

        }

        public virtual Task<bool> TryAddAsync(TKey key,
            TValue value,
            bool overwrite = false,
            CancellationToken cancellationToken = default)
        {
            return ExecuteWithReplicationAsync(m => m.TryAddAsync(key, value, overwrite));
        }

        public virtual Task<TValue> GetValueOrDefaultAsync(TKey key, CancellationToken cancellationToken = default)
        {
            return ExecuteWithFallbackAsync(m => m.GetValueOrDefaultAsync(key));
        }

        public virtual Task<bool> TryRemoveAsync(TKey key, CancellationToken cancellationToken = default)
        {
            return ExecuteWithReplicationAsync(m => m.TryRemoveAsync(key));
        }

        public virtual Task<bool> ContainsKeyAsync(TKey key, CancellationToken cancellationToken = default)
        {
            return ExecuteWithFallbackAsync(m => m.ContainsKeyAsync(key));
        }
    }
}
