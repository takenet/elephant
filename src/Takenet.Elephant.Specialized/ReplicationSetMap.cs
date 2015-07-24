using System;

namespace Takenet.Elephant.Specialized
{
    /// <summary>
    /// Implements a replication mechanism with a master and slave maps. 
    /// For write actions, the operation must succeed in both;
    /// For queries, if the action fails in the first, it falls back to the second.
    /// </summary>    
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public class ReplicationSetMap<TKey, TValue> : ReplicationMap<TKey, ISet<TValue>>, ISetMap<TKey, TValue>
    {
        public ReplicationSetMap(ISetMap<TKey, TValue> master, ISetMap<TKey, TValue> slave, TimeSpan synchronizationTimeout) : base(master, slave, synchronizationTimeout)
        {
        }

        public ReplicationSetMap(ISetMap<TKey, TValue> master, ISetMap<TKey, TValue> slave, ISynchronizer<IMap<TKey, ISet<TValue>>> synchronizer) : base(master, slave, synchronizer)
        {
        }
    }
}
