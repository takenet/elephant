using System;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Sql
{
    public class SqlSet<T> : SqlCollectionBase<T>, ISet<T>
    {
        public SqlSet(string connectionString, ITable table, IMapper<T> mapper)
            : this(new SqlDatabaseDriver(), connectionString, table, mapper)
        {
        }

        public SqlSet(IDatabaseDriver databaseDriver, string connectionString, ITable table, IMapper<T> mapper)
            : base(databaseDriver, connectionString, table, mapper)
        {
        } 

        public virtual async Task AddAsync(T value, CancellationToken cancellationToken = default)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            var columnValues = GetColumnValues(value, true);
            
            var keyColumnValues = GetKeyColumnValues(value, true);
            var nonIdentityKeyColumnValues = GetKeyColumnValues(keyColumnValues, false);
            var identityKeyColumnValues = GetKeyColumnValues(GetIdentityColumnValues(keyColumnValues), true);

            using (var cancellationTokenSource = CreateCancellationTokenSource(cancellationToken))
            {
                using (var connection = await GetConnectionAsync(cancellationTokenSource.Token).ConfigureAwait(false))
                {
                    // If there's a key, creates a merge command; otherwise:
                    //   If there's an identity column on the table, creates an insert that returns the inserted key columns; otherwise, just a regular insert.                    
                    DbCommand command;
                    string[] outputColumnNames = null;

                    // Merge if there's any non identity key or if there's any identity key with value that is not zero
                    if (nonIdentityKeyColumnValues.Count > 0 ||
                        (identityKeyColumnValues.Count > 0 && identityKeyColumnValues.Any(c => c.Value != null && c.Value.ToString() != "0"))) // Using string cast to avoid reflection for checking default values of short, int, long,.
                    {
                        command = connection.CreateMergeCommand(
                            DatabaseDriver, 
                            Table,
                            nonIdentityKeyColumnValues,
                            columnValues, 
                            identityKeyColumnValues);
                    }
                    else if (Table.Columns.Any(c => c.Value.IsIdentity))
                    {
                        // Note: We do not set non key identity columns
                        outputColumnNames =
                            Table.Columns.Where(c => c.Value.IsIdentity).Select(c => c.Key).ToArray();

                        command = connection.CreateInsertOutputCommand(
                            DatabaseDriver, 
                            Table,
                            columnValues,
                            outputColumnNames);
                    }
                    else
                    {
                        command = connection.CreateInsertCommand(DatabaseDriver, Table, columnValues);
                    }
                    
                    using (command)
                    {
                        if (outputColumnNames != null)
                        {
                            using (var reader = await command.ExecuteReaderAsync(cancellationTokenSource.Token).ConfigureAwait(false))
                            {
                                if (!await reader.ReadAsync(cancellationTokenSource.Token))
                                {
                                    throw new Exception("The database operation failed");
                                }

                                Mapper.Create(reader, outputColumnNames, value);
                            }
                        }
                        else if (await command.ExecuteNonQueryAsync(cancellationTokenSource.Token).ConfigureAwait(false) == 0)
                        {
                            throw new Exception("The database operation failed");
                        }
                    }
                    connection.Close();
                }
            }
        }

        public virtual async Task<bool> TryRemoveAsync(T value, CancellationToken cancellationToken = default)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            var keyColumnValues = GetKeyColumnValues(value, true);
            using (var cancellationTokenSource = CreateCancellationTokenSource(cancellationToken))
            {
                using (var connection = await GetConnectionAsync(cancellationTokenSource.Token).ConfigureAwait(false))
                {
                    return await TryRemoveAsync(keyColumnValues, connection, cancellationTokenSource.Token).ConfigureAwait(false);
                }
            }
        }        

        public virtual async Task<bool> ContainsAsync(T value, CancellationToken cancellationToken = default)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            var keyColumnValues = GetKeyColumnValues(value, true);
            using (var cancellationTokenSource = CreateCancellationTokenSource(cancellationToken))
            {
                using (var connection = await GetConnectionAsync(cancellationTokenSource.Token).ConfigureAwait(false))
                {
                    return await ContainsAsync(keyColumnValues, connection, cancellationTokenSource.Token).ConfigureAwait(false);
                }
            }
        }
       
    }
}
