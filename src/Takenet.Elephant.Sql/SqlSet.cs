using System;
using System.Linq;
using System.Threading.Tasks;
using Takenet.Elephant.Sql.Mapping;

namespace Takenet.Elephant.Sql
{
    public class SqlSet<T> : StorageBase<T>, ISet<T>
    {
        #region Constructors

        public SqlSet(string connectionString, ITable table, IMapper<T> mapper)
            : this(new SqlDatabaseDriver(), connectionString, table, mapper)
        {
        }

        public SqlSet(IDatabaseDriver databaseDriver, string connectionString, ITable table, IMapper<T> mapper)
            : base(databaseDriver, connectionString, table, mapper)
        {
        } 

        #endregion

        #region ISet<T> Members

        public virtual async Task AddAsync(T value)
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
                connection.Close();
            }
        }

        public virtual async Task<bool> TryRemoveAsync(T value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            var keyColumnValues = GetKeyColumnValues(value);
            var cancellationToken = CreateCancellationToken();
            using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
            {
                return await TryRemoveAsync(keyColumnValues, connection, cancellationToken).ConfigureAwait(false);
            }
        }

        public virtual Task<IAsyncEnumerable<T>> AsEnumerableAsync()
        {
            var selectColumns = Table.Columns.Keys.ToArray();
            return Task.FromResult<IAsyncEnumerable<T>>(
                new DbDataReaderAsyncEnumerable<T>(
                    GetConnectionAsync, 
                    c => c.CreateSelectCommand(Table.Name, null, selectColumns),
                    Mapper, 
                    selectColumns));
        }

        public virtual async Task<bool> ContainsAsync(T value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            var keyColumnValues = GetKeyColumnValues(value);
            var cancellationToken = CreateCancellationToken();
            using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
            {
                return await ContainsAsync(keyColumnValues, connection, cancellationToken).ConfigureAwait(false);
            }
        }

        public virtual async Task<long> GetLengthAsync()
        {
            var cancellationToken = CreateCancellationToken();
            using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
            {
                using (var countCommand = connection.CreateSelectCountCommand(Table.Name, filter: null))
                {
                    return (int)await countCommand.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                }                    
            }
        }

        #endregion
    }
}
