using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Sql
{
    /// <summary>
    /// Implements a <see cref="SqlMap{TKey,TItem}"/> that uses distinct connections for read and write operations,
    /// providing the adequate "ApplicationIntent" parameter to the connection string in each case.
    /// This is useful when there is replica databases that can be used for read operations, reducing the overhead in the main instances.
    /// More info:
    /// https://docs.microsoft.com/en-us/sql/database-engine/availability-groups/windows/secondary-replica-connection-redirection-always-on-availability-groups
    /// </summary>
    public class ApplicationIntentSqlMap<TKey, TValue> : ApplicationIntentStorageBase, 
        IKeysMap<TKey, TValue>,
        IMap<TKey, TValue>,
        IPropertyMap<TKey, TValue>,
        IUpdatableMap<TKey, TValue>
    {
        private readonly SqlMap<TKey, TValue> _readOnlyMap;
        private readonly SqlMap<TKey, TValue> _writeMap;
        
        public ApplicationIntentSqlMap(IDatabaseDriver databaseDriver, string connectionString, ITable table, IMapper<TKey> keyMapper, IMapper<TValue> valueMapper) 
            : base(databaseDriver, connectionString, table)
        {
            _readOnlyMap = new SqlMap<TKey, TValue>(databaseDriver, ReadOnlyConnectionString, ReadOnlyTable, keyMapper, valueMapper);
            _writeMap = new SqlMap<TKey, TValue>(databaseDriver, connectionString, table, keyMapper, valueMapper);
        }
        
        public virtual async Task<bool> TryAddAsync(TKey key, TValue value, bool overwrite = false,
            CancellationToken cancellationToken = new CancellationToken()) => await _writeMap.TryAddAsync(key, value, overwrite, cancellationToken);
    
        public virtual async Task<bool> TryRemoveAsync(TKey key, CancellationToken cancellationToken = new CancellationToken()) => await _writeMap.TryRemoveAsync(key, cancellationToken);

        public virtual async Task MergeAsync(TKey key, TValue value, CancellationToken cancellationToken = new CancellationToken()) => await _writeMap.MergeAsync(key, value, cancellationToken);

        public virtual async Task<bool> TryUpdateAsync(TKey key, TValue newValue, TValue oldValue) => await _writeMap.TryUpdateAsync(key, newValue, oldValue);
    
        public virtual async Task<TValue> GetValueOrDefaultAsync(TKey key, CancellationToken cancellationToken = new CancellationToken())
        {
            await SynchronizeSchemaAsync(cancellationToken);
            return await _readOnlyMap.GetValueOrDefaultAsync(key, cancellationToken);
        }
        
        public virtual async Task<bool> ContainsKeyAsync(TKey key, CancellationToken cancellationToken = new CancellationToken())
        {
            await SynchronizeSchemaAsync(cancellationToken);
            return await _readOnlyMap.ContainsKeyAsync(key, cancellationToken);
        }

        public virtual async Task<IAsyncEnumerable<TKey>> GetKeysAsync()
        {
            await SynchronizeSchemaAsync(default);
            return await _readOnlyMap.GetKeysAsync();
        }

        public virtual async Task SetPropertyValueAsync<TProperty>(TKey key, string propertyName, TProperty propertyValue,
            CancellationToken cancellationToken = new CancellationToken())
        {
            await SynchronizeSchemaAsync(cancellationToken);
            await _readOnlyMap.SetPropertyValueAsync(key, propertyName, propertyValue, cancellationToken);
        }

        public virtual async Task<TProperty> GetPropertyValueOrDefaultAsync<TProperty>(TKey key, string propertyName,
            CancellationToken cancellationToken = new CancellationToken())
        {
            await SynchronizeSchemaAsync(cancellationToken);
            return await _readOnlyMap.GetPropertyValueOrDefaultAsync<TProperty>(key, propertyName, cancellationToken);
        }
    }
}