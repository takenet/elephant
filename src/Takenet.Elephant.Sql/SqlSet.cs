using System;
using System.Linq;
using System.Threading.Tasks;
using Takenet.Elephant.Sql.Mapping;

namespace Takenet.Elephant.Sql
{
    public class SqlSet<T> : StorageBase<T>, ISet<T>
    {
        public SqlSet(IDatabaseDriver databaseDriver, string connectionString, ITable table, IMapper<T> mapper)
            : base(databaseDriver, connectionString, table, mapper)
        {
        }

        #region ISet<T> Members

        public async Task AddAsync(T value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            var cancellationToken = CreateCancellationToken();
            var columnValues = GetColumnValues(value);
            var keyColumnValues = GetKeyColumnValues(columnValues);

            using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
            {                
                using (var command = connection.CreateInsertWhereNotExistsCommand(Table.Name, keyColumnValues, columnValues, true))
                {
                    if (await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 0)
                    {
                        throw new Exception("The database operation failed");
                    }
                }
            }
        }

        public async Task<bool> TryRemoveAsync(T value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            var keyColumnValues = GetKeyColumnValues(value);
            var cancellationToken = CreateCancellationToken();
            using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
            {
                return await TryRemoveAsync(keyColumnValues, connection, cancellationToken).ConfigureAwait(false);
            }
        }

        public async Task<IAsyncEnumerable<T>> AsEnumerableAsync()
        {
            var cancellationToken = CreateCancellationToken();
            var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);            
            var selectColumns = Table.Columns.Keys.ToArray();
            var command = connection.CreateSelectCommand(Table.Name, null, selectColumns);
            return new DbDataReaderAsyncEnumerable<T>(command, Mapper, selectColumns);
        }

        public async Task<bool> ContainsAsync(T value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            var keyColumnValues = GetKeyColumnValues(value);
            var cancellationToken = CreateCancellationToken();
            using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
            {
                return await ContainsAsync(keyColumnValues, connection, cancellationToken).ConfigureAwait(false);
            }
        }

        #endregion
    }
}
