using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Takenet.SimplePersistence.Sql.Mapping;

namespace Takenet.SimplePersistence.Sql
{
    public abstract class MapStorageBase<TKey, TValue> : StorageBase<TValue>
    {
        protected MapStorageBase(ITable table, string connectionString) 
            : base(table, connectionString)
        {

        }

        protected abstract IMapper<TKey> KeyMapper { get; }

        protected virtual IDictionary<string, object> GetColumnValues(TKey key, TValue value)
        {
            return KeyMapper
                .GetColumnValues(key)
                .Concat(GetColumnValues(value))
                .ToDictionary(t => t.Key, t => t.Value);
        }

        protected virtual Task<bool> TryRemoveAsync(TKey key, DbConnection connection, CancellationToken cancellationToken, SqlTransaction sqlTransaction = null)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            var keyColumnValues = KeyMapper.GetColumnValues(key);
            return TryRemoveAsync(keyColumnValues, connection, cancellationToken, sqlTransaction);
            
        }

        protected virtual Task<bool> ContainsKeyAsync(TKey key, DbConnection connection, CancellationToken cancellationToken)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            var keyColumnValues = KeyMapper.GetColumnValues(key);
            return ContainsAsync(keyColumnValues, connection, cancellationToken);            
        }

        protected virtual Task<IAsyncEnumerable<TKey>> GetKeysAsync(DbConnection connection, CancellationToken cancellationToken)
        {
            var selectColumns = Table.KeyColumns;
            var command = connection.CreateSelectCommand(Table.Name, null, selectColumns);            
            return Task.FromResult<IAsyncEnumerable<TKey>>(
                new DbDataReaderAsyncEnumerable<TKey>(command, KeyMapper, selectColumns));
        }
    }
}