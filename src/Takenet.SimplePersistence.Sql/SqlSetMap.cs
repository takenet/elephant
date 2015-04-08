using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Takenet.SimplePersistence.Sql.Mapping;

namespace Takenet.SimplePersistence.Sql
{
    public abstract class SqlSetMap<TKey, TItem> : MapStorageBase<TKey, TItem>, ISetMap<TKey, TItem>
    {
        private readonly IsolationLevel _addIsolationLevel;

        protected SqlSetMap(ITable table, string connectionString, IsolationLevel addIsolationLevel = IsolationLevel.ReadCommitted) 
            : base(table, connectionString)
        {
            _addIsolationLevel = addIsolationLevel;
        }

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
                return new InternalSet(keyColumnValues, Mapper, Table, ConnectionString);
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

        private class InternalSet : SqlSet<TItem>
        {
            public IDictionary<string, object> MapKeyColumnValues { get; }

            public InternalSet(IDictionary<string, object> mapKeyColumnValues, IMapper<TItem> mapper, ITable table, string connectionString) 
                : base(table, connectionString)
            {
                MapKeyColumnValues = mapKeyColumnValues;
                Mapper = mapper;
            }

            protected override IMapper<TItem> Mapper { get; }

            protected override IDictionary<string, object> GetColumnValues(TItem entity)
            {
                return MapKeyColumnValues.Concat(base.GetColumnValues(entity)).ToDictionary(k => k.Key, k => k.Value);
            }
        }
    }
}
