using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Sql
{
    /// <summary>
    /// Implements a <see cref="SqlSetMap{TKey,TItem}"/> that uses distinct connections for read and write operations,
    /// providing the adequate "ApplicationIntent" parameter to the connection string in each case.
    /// This is useful when there is replica databases that can be used for read operations, reducing the overhead in the main instances.
    /// More info:
    /// https://docs.microsoft.com/en-us/sql/database-engine/availability-groups/windows/secondary-replica-connection-redirection-always-on-availability-groups
    /// </summary>
    public class ApplicationIntentSqlSetMap<TKey, TItem> : ApplicationIntentStorageBase, IItemSetMap<TKey, TItem>,
        IKeysMap<TKey, ISet<TItem>>
    {
        private readonly SqlSetMap<TKey, TItem> _readOnlySetMap;
        private readonly SqlSetMap<TKey, TItem> _writeSetMap;

        public ApplicationIntentSqlSetMap(
            IDatabaseDriver databaseDriver,
            string connectionString,
            ITable table,
            IMapper<TKey> keyMapper,
            IMapper<TItem> valueMapper,
            IsolationLevel addIsolationLevel = IsolationLevel.ReadCommitted)
            : base(databaseDriver, connectionString, table)
        {
            _readOnlySetMap = new SqlSetMap<TKey, TItem>(databaseDriver, ReadOnlyConnectionString, ReadOnlyTable,
                keyMapper, valueMapper, addIsolationLevel);
            _writeSetMap = new SqlSetMap<TKey, TItem>(databaseDriver, connectionString, table, keyMapper, valueMapper,
                addIsolationLevel);
        }

        public virtual async Task<bool> TryAddAsync(TKey key, ISet<TItem> value, bool overwrite = false,
            CancellationToken cancellationToken = default) =>
            await _writeSetMap.TryAddAsync(key, value, overwrite, cancellationToken);

        public virtual async Task<bool> TryRemoveAsync(TKey key, CancellationToken cancellationToken = default) =>
            await _writeSetMap.TryRemoveAsync(key, cancellationToken);

        public virtual async Task<ISet<TItem>> GetValueOrDefaultAsync(TKey key, CancellationToken cancellationToken = default)
        {
            await SynchronizeSchemaAsync(default);
            return await _readOnlySetMap.GetValueOrDefaultAsync(key, cancellationToken);
        }

        public virtual async Task<bool> ContainsKeyAsync(TKey key, CancellationToken cancellationToken = default)
        {
            await SynchronizeSchemaAsync(cancellationToken);
            return await _readOnlySetMap.ContainsKeyAsync(key, cancellationToken);
        }

        public virtual async Task<ISet<TItem>> GetValueOrEmptyAsync(TKey key, CancellationToken cancellationToken = default)
        {
            await SynchronizeSchemaAsync(cancellationToken);
            return await _readOnlySetMap.GetValueOrEmptyAsync(key, cancellationToken);
        }

        public virtual async Task<TItem> GetItemOrDefaultAsync(TKey key, TItem item)
        {
            await SynchronizeSchemaAsync(default);
            return await _readOnlySetMap.GetItemOrDefaultAsync(key, item);
        }

        public virtual async Task<IAsyncEnumerable<TKey>> GetKeysAsync()
        {
            await SynchronizeSchemaAsync(default);
            return await _readOnlySetMap.GetKeysAsync();
        }
    }
}