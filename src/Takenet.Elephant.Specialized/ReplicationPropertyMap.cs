using System;
using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized
{
    /// <summary>
    /// Implements a replication mechanism with a primary and slave maps. 
    /// For write actions, the operation must succeed in both;
    /// For queries, if the action fails in the first, it falls back to the second.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public class ReplicationPropertyMap<TKey, TValue> : ReplicationStrategy<IPropertyMap<TKey, TValue>>, IPropertyMap<TKey, TValue>
    {
        public ReplicationPropertyMap(IPropertyMap<TKey, TValue> master, IPropertyMap<TKey, TValue> slave, TimeSpan synchronizationTimeout)
            : this(master, slave, new DifferentialMapSynchronizer<TKey, TValue>(synchronizationTimeout))
        {

        }

        public ReplicationPropertyMap(IPropertyMap<TKey, TValue> master, IPropertyMap<TKey, TValue> slave, ISynchronizer<IPropertyMap<TKey, TValue>> synchronizer) 
            : base(master, slave, synchronizer)
        {
            
        }

        public virtual Task<bool> TryAddAsync(TKey key, TValue value, bool overwrite = false)
        {
            return ExecuteWithReplicationAsync(m => m.TryAddAsync(key, value, overwrite));
        }

        public virtual Task<TValue> GetValueOrDefaultAsync(TKey key)
        {
            return ExecuteWithFallbackAsync(m => m.GetValueOrDefaultAsync(key));
        }

        public virtual Task<bool> TryRemoveAsync(TKey key)
        {
            return ExecuteWithReplicationAsync(m => m.TryRemoveAsync(key));
        }

        public virtual Task<bool> ContainsKeyAsync(TKey key)
        {
            return ExecuteWithFallbackAsync(m => m.ContainsKeyAsync(key));
        }

        public virtual Task SetPropertyValueAsync<TProperty>(TKey key, string propertyName, TProperty propertyValue)
        {
            return ExecuteWithReplicationAsync(m => m.SetPropertyValueAsync(key, propertyName, propertyValue));
        }

        public virtual Task<TProperty> GetPropertyValueOrDefaultAsync<TProperty>(TKey key, string propertyName)
        {
            return ExecuteWithFallbackAsync(m => m.GetPropertyValueOrDefaultAsync<TProperty>(key, propertyName));
        }

        public virtual Task MergeAsync(TKey key, TValue value)
        {
            return ExecuteWithReplicationAsync(m => m.MergeAsync(key, value));
        }
    }
}