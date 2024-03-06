using System;
using System.Linq;
using System.Threading.Tasks;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Sql
{
    public class SqlList<T> : SqlCollectionBase<T>, IList<T>
    {
        #region Constructors

        public SqlList(string connectionString, ITable table, IMapper<T> mapper)
            : this(new SqlDatabaseDriver(), connectionString, table, mapper)
        {
        }

        public SqlList(IDatabaseDriver databaseDriver, string connectionString, ITable table, IMapper<T> mapper)
            : base(databaseDriver, connectionString, table, mapper)
        {
            // If there is a key in the table, at least one of the key columns must be identity
            if (table.KeyColumnsNames.Any() &&
                !table.KeyColumnsNames.Any(c => table.Columns[c].IsIdentity))
            {
                throw new ArgumentException("The table must contain an key identity column", nameof(table));
            }
        }

        #endregion

        public virtual async Task AddAsync(T value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            var columnValues = GetColumnValues(value);

            using (var cancellationTokenSource = CreateCancellationTokenSource())
            {
                using (var connection = await GetConnectionAsync(cancellationTokenSource.Token).ConfigureAwait(false))
                {
                    using (var command = connection.CreateInsertCommand(DatabaseDriver, Table, columnValues))
                    {
                        if (await command.ExecuteNonQueryAsync(cancellationTokenSource.Token).ConfigureAwait(false) == 0)
                        {
                            throw new Exception("The database operation failed");
                        }
                    }
                    connection.Close();
                }
            }
        }
        
        public virtual async Task<long> RemoveAllAsync(T value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            var keyColumnValues = GetKeyColumnValues(value, includeIdentityTypes: true);
            using (var cancellationTokenSource = CreateCancellationTokenSource())
            {
                using (var connection = await GetConnectionAsync(cancellationTokenSource.Token).ConfigureAwait(false))
                {
                    using (var deleteCommand = connection.CreateDeleteCommand(DatabaseDriver, Table, keyColumnValues))
                    {
                        return await deleteCommand.ExecuteNonQueryAsync(cancellationTokenSource.Token).ConfigureAwait(false);
                    }
                }
            }
        }
    }
}
