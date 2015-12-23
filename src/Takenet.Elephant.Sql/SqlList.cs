using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Takenet.Elephant.Sql.Mapping;

namespace Takenet.Elephant.Sql
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
        }

        #endregion

        public async Task AddAsync(T value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            var columnValues = GetColumnValues(value);

            using (var cancellationTokenSource = CreateCancellationTokenSource())
            {
                using (var connection = await GetConnectionAsync(cancellationTokenSource.Token).ConfigureAwait(false))
                {
                    using (var command = connection.CreateInsertCommand(Table.Name, columnValues))
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
        
        public async Task<long> RemoveAllAsync(T value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            var keyColumnValues = GetKeyColumnValues(value);
            using (var cancellationTokenSource = CreateCancellationTokenSource())
            {
                using (var connection = await GetConnectionAsync(cancellationTokenSource.Token).ConfigureAwait(false))
                {
                    using (var deleteCommand = connection.CreateDeleteCommand(Table.Name, keyColumnValues))
                    {
                        return await deleteCommand.ExecuteNonQueryAsync(cancellationTokenSource.Token).ConfigureAwait(false);
                    }                    
                }
            }
        }
    }
}
