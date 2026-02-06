using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Sql
{
    public abstract class SqlCollectionBase<T> : StorageBase<T>, ICollection<T>
    {        
        protected SqlCollectionBase(IDatabaseDriver databaseDriver, string connectionString, ITable table, IMapper<T> mapper, SqlRetryLogicOption retryOptions = null)
            : base(databaseDriver, connectionString, table, mapper, retryOptions)
        {

        }

        public virtual IAsyncEnumerable<T> AsEnumerableAsync(CancellationToken cancellationToken = default)
        {
            var selectColumns = Table.Columns.Keys.ToArray();
            return new DbDataReaderAsyncEnumerable<T>(
                GetConnectionAsync,
                c => c.CreateSelectCommand(DatabaseDriver, Table, null, selectColumns),
                Mapper,
                selectColumns,
                UseFullyAsyncEnumerator);
        }

        public virtual async Task<long> GetLengthAsync(CancellationToken cancellationToken = default)
        {
            using (var cancellationTokenSource = CreateCancellationTokenSource(cancellationToken))
            {
                using (var connection = await GetConnectionAsync(cancellationTokenSource.Token).ConfigureAwait(false))
                {
                    using (var countCommand = connection.CreateSelectCountCommand(DatabaseDriver, Table, filter: null))
                    {
                        return Convert.ToInt64(
                            await countCommand.ExecuteScalarAsync(cancellationTokenSource.Token).ConfigureAwait(false));
                    }
                }
            }
        }
    }
}
