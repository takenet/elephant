using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Sql
{
    public class SqlSetMap<TKey, TItem> : MapStorageBase<TKey, TItem>, IItemSetMap<TKey, TItem>, IKeysMap<TKey, ISet<TItem>>
    {
        private readonly IsolationLevel _addIsolationLevel;

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

        public virtual async Task<bool> TryAddAsync(TKey key,
            ISet<TItem> value,
            bool overwrite = false,
            CancellationToken cancellationToken = default)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (value == null) throw new ArgumentNullException(nameof(value));
            var keyColumnValues = GetKeyColumnValues(KeyMapper.GetColumnValues(key));

            var internalSet = value as InternalSet;
            if (internalSet != null)
            {
                return keyColumnValues.SequenceEqual(internalSet.MapKeyColumnValues) && overwrite;
            }

            using (var cancellationTokenSource = CreateCancellationTokenSource())
            {
                using (var connection = await GetConnectionAsync(cancellationTokenSource.Token).ConfigureAwait(false))
                {
                    if (!overwrite &&
                        await ContainsAsync(keyColumnValues, connection, cancellationTokenSource.Token))
                    {
                        return false;
                    }

                    using (var transaction = connection.BeginTransaction(_addIsolationLevel))
                    {
                        if (overwrite)
                        {
                            await
                                TryRemoveAsync(keyColumnValues, connection, cancellationTokenSource.Token, transaction)
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
                                    var command = connection.CreateInsertWhereNotExistsCommand(DatabaseDriver, Table.Schema, Table.Name, itemKeyColumnValues, columnValues))
                                {
                                    command.Transaction = transaction;
                                    success =
                                        await command.ExecuteNonQueryAsync(cancellationTokenSource.Token).ConfigureAwait(false) > 0;
                                }
                            },
                            cancellationTokenSource.Token);

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
        }

        public virtual async Task<ISet<TItem>> GetValueOrDefaultAsync(TKey key,
            CancellationToken cancellationToken = default)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            using (var cancellationTokenSource = CreateCancellationTokenSource())
            {
                using (var connection = await GetConnectionAsync(cancellationTokenSource.Token).ConfigureAwait(false))
                {
                    var keyColumnValues = KeyMapper.GetColumnValues(key);
                    if (!await ContainsAsync(keyColumnValues, connection, cancellationTokenSource.Token))
                    {
                        return null;
                    }
                    return new InternalSet(ConnectionString, Table, Mapper, DatabaseDriver, keyColumnValues);
                }
            }
        }

        public virtual Task<ISet<TItem>> GetValueOrEmptyAsync(TKey key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));            
            var keyColumnValues = KeyMapper.GetColumnValues(key);
            return new InternalSet(ConnectionString, Table, Mapper, DatabaseDriver, keyColumnValues).AsCompletedTask<ISet<TItem>>();
        }

        public virtual async Task<bool> TryRemoveAsync(TKey key, CancellationToken cancellationToken = default)
        {
            using (var cancellationTokenSource = CreateCancellationTokenSource())
            {
                using (var connection = await GetConnectionAsync(cancellationTokenSource.Token).ConfigureAwait(false))
                {
                    return await TryRemoveAsync(key, connection, cancellationTokenSource.Token);
                }
            }
        }

        public virtual async Task<bool> ContainsKeyAsync(TKey key, CancellationToken cancellationToken = default)
        {
            using (var cancellationTokenSource = CreateCancellationTokenSource())
            {
                using (var connection = await GetConnectionAsync(cancellationTokenSource.Token).ConfigureAwait(false))
                {
                    return await ContainsKeyAsync(key, connection, cancellationTokenSource.Token);
                }
            }
        }

        public virtual async Task<TItem> GetItemOrDefaultAsync(TKey key, TItem item)
        {
            var keyColumnValues = GetKeyColumnValues(GetColumnValues(key, item));
            var selectColumns = Table.Columns.Keys.ToArray();
            using (var cancellationTokenSource = CreateCancellationTokenSource())
            {
                return await new DbDataReaderAsyncEnumerable<TItem>(
                            GetConnectionAsync,
                            c => c.CreateSelectCommand(DatabaseDriver, Table.Schema, Table.Name, keyColumnValues, selectColumns),
                            Mapper,
                            selectColumns)
                            .FirstOrDefaultAsync(cancellationTokenSource.Token);
            }
        }

        public virtual Task<IAsyncEnumerable<TKey>> GetKeysAsync()
        {
            var selectColumns = Table.KeyColumnsNames;
            return Task.FromResult<IAsyncEnumerable<TKey>>(
                new DbDataReaderAsyncEnumerable<TKey>(
                    GetConnectionAsync, 
                    c => c.CreateSelectCommand(DatabaseDriver, Table.Schema, Table.Name, null, selectColumns),
                    KeyMapper, 
                    selectColumns));
        }

        private class InternalSet : SqlSet<TItem>
        {
            public IDictionary<string, object> MapKeyColumnValues { get; }

            public InternalSet(string connectionString, ITable table, IMapper<TItem> mapper, IDatabaseDriver databaseDriver, IDictionary<string, object> mapKeyColumnValues) 
                : base(databaseDriver, connectionString, table, mapper)
            {
                MapKeyColumnValues = mapKeyColumnValues;
            }

            protected override IDictionary<string, object> GetColumnValues(TItem entity, bool emitNullValues = false, bool includeIdentityTypes = false)
            {
                return MapKeyColumnValues
                    .Union(base.GetColumnValues(entity, emitNullValues, includeIdentityTypes))
                    .ToDictionary(k => k.Key, k => k.Value);
            }

            public override Task<IAsyncEnumerable<TItem>> AsEnumerableAsync(CancellationToken cancellationToken =
                default)
            {                                
                var selectColumns = Table.Columns.Keys.ToArray();                
                return Task.FromResult<IAsyncEnumerable<TItem>>(
                    new DbDataReaderAsyncEnumerable<TItem>(
                        GetConnectionAsync, 
                        c => c.CreateSelectCommand(DatabaseDriver, Table.Schema, Table.Name, MapKeyColumnValues, selectColumns),
                        Mapper, 
                        selectColumns));
            }

            public override async Task<long> GetLengthAsync(CancellationToken cancellationToken = default)
            {
                using (var cancellationTokenSource = CreateCancellationTokenSource())
                {
                    using (var connection = await GetConnectionAsync(cancellationTokenSource.Token).ConfigureAwait(false))
                    {
                        using (var countCommand = connection.CreateSelectCountCommand(DatabaseDriver, Table.Schema, Table.Name, MapKeyColumnValues))
                        {
                            var result = await countCommand.ExecuteScalarAsync(cancellationTokenSource.Token).ConfigureAwait(false);
                            // In postgre, it is a long; in sql server, a int32...
                            return Convert.ToInt64(result);
                        }
                    }
                }
            }

            protected override Task<QueryResult<TItem>> QueryAsync<TResult>(
                string filter, 
                string[] selectColumns, 
                int skip, 
                int take, 
                CancellationToken cancellationToken,
                string[] orderByColumns,
                bool orderByAscending = true,
                IDictionary<string, object> filterValues = null,
                bool distinct = false)
            {
                if (filterValues == null)
                {
                    filterValues = MapKeyColumnValues;
                }
                else
                {
                    filterValues = filterValues
                        .Union(MapKeyColumnValues)
                        .ToDictionary(k => k.Key, v => v.Value);
                }

                return base.QueryAsync<TResult>(filter, selectColumns, skip, take, cancellationToken, orderByColumns, orderByAscending, filterValues, distinct);
            }
        }
    }
}
