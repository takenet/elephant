using System;
using System.Linq;
using System.Threading.Tasks;
using Takenet.Elephant.Sql.Mapping;

namespace Takenet.Elephant.Sql
{
    public class SqlSet<T> : SqlCollectionBase<T>, ISet<T>
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
            var columnValues = GetColumnValues(value);
            var keyColumnValues = GetKeyColumnValues(columnValues);

            using (var cancellationTokenSource = CreateCancellationTokenSource())
            {
                using (var connection = await GetConnectionAsync(cancellationTokenSource.Token).ConfigureAwait(false))
                {
                    using (var command = connection.CreateMergeCommand(DatabaseDriver, Table.Name, keyColumnValues, columnValues))
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

        public virtual async Task<bool> TryRemoveAsync(T value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            var keyColumnValues = GetKeyColumnValues(value);
            using (var cancellationTokenSource = CreateCancellationTokenSource())
            {
                using (var connection = await GetConnectionAsync(cancellationTokenSource.Token).ConfigureAwait(false))
                {
                    return await TryRemoveAsync(keyColumnValues, connection, cancellationTokenSource.Token).ConfigureAwait(false);
                }
            }
        }        

        public virtual async Task<bool> ContainsAsync(T value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            var keyColumnValues = GetKeyColumnValues(value);
            using (var cancellationTokenSource = CreateCancellationTokenSource())
            {
                using (var connection = await GetConnectionAsync(cancellationTokenSource.Token).ConfigureAwait(false))
                {
                    return await ContainsAsync(keyColumnValues, connection, cancellationTokenSource.Token).ConfigureAwait(false);
                }
            }
        }
       
        #endregion
    }
}
