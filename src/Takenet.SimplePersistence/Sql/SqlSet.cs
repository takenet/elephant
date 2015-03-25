using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Takenet.SimplePersistence.Sql.Mapping;

namespace Takenet.SimplePersistence.Sql
{
    public abstract class SqlSet<T> : StorageBase<T>, ISet<T>
    {
        protected SqlSet(ITable table, string connectionString)
            : base(table, connectionString)
        {

        }

        #region ISet<T> Members

        public async Task AddAsync(T value)
        {
            var cancellationToken = CreateCancellationToken();
            var columnValues = Mapper.GetColumnValues(value);

            using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
            {
                using (var command = connection.CreateTextCommand(
                    SqlTemplates.Insert,
                    new
                    {
                        tableName = Table.TableName.AsSqlIdentifier(),
                        columns = columnValues.Keys.Select(c => c.AsSqlIdentifier()).ToCommaSepparate(),
                        values = columnValues.Keys.Select(v => v.AsSqlParameterName()).ToCommaSepparate()
                    },
                    columnValues.Select(c => c.ToSqlParameter())))
                {
                    if (await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 0)
                    {
                        throw new InvalidOperationException("The database operation failed");
                    }
                }
            }
        }

        public async Task<bool> TryRemoveAsync(T value)
        {
            var keyValues = GetKeyColumnValues(value);
            var cancellationToken = CreateCancellationToken();
            using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
            {
                return await TryRemoveAsync(keyValues, connection, cancellationToken).ConfigureAwait(false);
            }
        }

        public async Task<IAsyncEnumerable<T>> AsEnumerableAsync()
        {
            var cancellationToken = CreateCancellationToken();
            var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);            
            var selectColumns = Table.Columns.Keys.ToArray();
            var command = connection.CreateTextCommand(
                SqlTemplates.Select,
                new
                {
                    columns = selectColumns.Select(c => c.AsSqlIdentifier()).ToCommaSepparate(),
                    tableName = Table.TableName.AsSqlIdentifier(),
                    filter = "1 = 1"
                });
            return new SqlDataReaderAsyncEnumerable<T>(command, Mapper, selectColumns);
        }

        public async Task<bool> ContainsAsync(T value)
        {
            var keyValues = GetKeyColumnValues(value);
            var cancellationToken = CreateCancellationToken();
            using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
            {
                return await ContainsKeyAsync(keyValues, connection, cancellationToken).ConfigureAwait(false);
            }
        }

        #endregion
    }
}
