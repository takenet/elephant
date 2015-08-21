using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Takenet.Elephant.Sql.Mapping;

namespace Takenet.Elephant.Sql
{
    public class SqlSetMap<TKey, TItem> : MapStorageBase<TKey, TItem>, IItemSetMap<TKey, TItem>, IKeysMap<TKey, ISet<TItem>>
    {
        private readonly IsolationLevel _addIsolationLevel;

        #region Constructor

        public SqlSetMap(string connectionString, ITable table, IMapper<TKey> keyMapper, IMapper<TItem> valueMapper, IsolationLevel addIsolationLevel = IsolationLevel.ReadCommitted)
            : this(new SqlDatabaseDriver(), connectionString, table, keyMapper, valueMapper)
        {
            _addIsolationLevel = addIsolationLevel;
        }

        public SqlSetMap(IDatabaseDriver databaseDriver, string connectionString, ITable table, IMapper<TKey> keyMapper, IMapper<TItem> valueMapper, IsolationLevel addIsolationLevel = IsolationLevel.ReadCommitted)
            : base(databaseDriver, connectionString, table, keyMapper, valueMapper)
        {
            _addIsolationLevel = addIsolationLevel;
        }

        #endregion

        #region ISetMap<TKey, TItem> Members

        public async Task<bool> TryAddAsync(TKey key, ISet<TItem> value, bool overwrite = false)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (value == null) throw new ArgumentNullException(nameof(value));
            var cancellationToken = CreateCancellationToken();
            var keyColumnValues = GetKeyColumnValues(KeyMapper.GetColumnValues(key));

            var internalSet = value as InternalSet;
            if (internalSet != null)
            {
                return keyColumnValues.SequenceEqual(internalSet.MapKeyColumnValues) && overwrite;
            }

            using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
            {
                if (!overwrite &&
                    await ContainsAsync(keyColumnValues, connection, cancellationToken))
                {
                    return false;
                }

                using (var transaction = connection.BeginTransaction(_addIsolationLevel))
                {
                    if (overwrite)
                    {
                        await
                            TryRemoveAsync(keyColumnValues, connection, cancellationToken, transaction)
                                .ConfigureAwait(false);
                    }

                    var success = true;
                    var items = await value.AsEnumerableAsync().ConfigureAwait(false);
                    await items.ForEachAsync(
                        async item =>
                        {
                            if (!success) return;
                            var columnValues = GetColumnValues(key, item);
                            var itemKeyColumnValues = GetKeyColumnValues(columnValues);

                            using (
                                var command = connection.CreateInsertWhereNotExistsCommand(Table.Name,
                                    itemKeyColumnValues, columnValues))
                            {
                                command.Transaction = transaction;
                                success =
                                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
                            }
                        },
                        cancellationToken);

                    if (success)
                    {
                        transaction.Commit();
                    }
                    else
                    {
                        transaction.Rollback();
                    }

                    return success;
                }
            }
        }

        public async Task<ISet<TItem>> GetValueOrDefaultAsync(TKey key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            var cancellationToken = CreateCancellationToken();
            using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
            {
                var keyColumnValues = KeyMapper.GetColumnValues(key);
                if (!await ContainsAsync(keyColumnValues, connection, cancellationToken))
                {
                    return null;
                }
                return new InternalSet(ConnectionString, Table, Mapper, DatabaseDriver, keyColumnValues);
            }
        }

        public async Task<bool> TryRemoveAsync(TKey key)
        {
            var cancellationToken = CreateCancellationToken();
            using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
            {
                return await TryRemoveAsync(key, connection, cancellationToken);
            }
        }

        public async Task<bool> ContainsKeyAsync(TKey key)
        {
            var cancellationToken = CreateCancellationToken();
            using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
            {
                return await ContainsKeyAsync(key, connection, cancellationToken);
            }
        }

        #endregion

        #region IItemSetMap<TKey, TItem> Members

        public virtual Task<TItem> GetItemOrDefaultAsync(TKey key, TItem value)
        {
            var cancellationToken = CreateCancellationToken();            
            var keyColumnValues = GetKeyColumnValues(GetColumnValues(key, value));
            var selectColumns = Table.Columns.Keys.ToArray();                
            return new DbDataReaderAsyncEnumerable<TItem>(
                GetConnectionAsync, 
                c => c.CreateSelectCommand(Table.Name, keyColumnValues, selectColumns), 
                Mapper, 
                selectColumns)
                .FirstOrDefaultAsync(cancellationToken);                            
        }

        #endregion

        #region IKeysMap<TKey, ISet<TItem>> Members

        public Task<IAsyncEnumerable<TKey>> GetKeysAsync()
        {
            var selectColumns = Table.KeyColumnsNames;
            return Task.FromResult<IAsyncEnumerable<TKey>>(
                new DbDataReaderAsyncEnumerable<TKey>(GetConnectionAsync, c => c.CreateSelectCommand(Table.Name, null, selectColumns), KeyMapper, selectColumns));
        }

        #endregion

        private class InternalSet : SqlSet<TItem>
        {
            public IDictionary<string, object> MapKeyColumnValues { get; }

            public InternalSet(string connectionString, ITable table, IMapper<TItem> mapper, IDatabaseDriver databaseDriver, IDictionary<string, object> mapKeyColumnValues) 
                : base(databaseDriver, connectionString, table, mapper)
            {
                MapKeyColumnValues = mapKeyColumnValues;
            }

            protected override IDictionary<string, object> GetColumnValues(TItem entity)
            {
                return MapKeyColumnValues
                    .Union(base.GetColumnValues(entity))
                    .ToDictionary(k => k.Key, k => k.Value);
            }

            public override Task<IAsyncEnumerable<TItem>> AsEnumerableAsync()
            {                                
                var selectColumns = Table.Columns.Keys.ToArray();                
                return Task.FromResult<IAsyncEnumerable<TItem>>(
                    new DbDataReaderAsyncEnumerable<TItem>(
                        GetConnectionAsync, 
                        c => c.CreateSelectCommand(Table.Name, MapKeyColumnValues, selectColumns), 
                        Mapper, 
                        selectColumns));
            }

            public override async Task<long> GetLengthAsync()
            {
                var cancellationToken = CreateCancellationToken();
                using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
                {
                    using (var countCommand = connection.CreateSelectCountCommand(Table.Name, MapKeyColumnValues))
                    {
                        return (int)await countCommand.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                    }
                }
            }
        }
    }
}
