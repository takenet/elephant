using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Sql
{
    public abstract class SqlCollectionBase<T> : StorageBase<T>, ICollection<T>
    {        
        protected SqlCollectionBase(IDatabaseDriver databaseDriver, string connectionString, ITable table, IMapper<T> mapper)
            : base(databaseDriver, connectionString, table, mapper)
        {

        }

        public virtual Task<IAsyncEnumerable<T>> AsEnumerableAsync(CancellationToken cancellationToken = default)
        {
            var selectColumns = Table.Columns.Keys.ToArray();
            return Task.FromResult<IAsyncEnumerable<T>>(
                new DbDataReaderAsyncEnumerable<T>(
                    GetConnectionAsync,
                    c => c.CreateSelectCommand(DatabaseDriver, Table.Schema, Table.Name, null, Table.Columns, selectColumns),
                    Mapper,
                    selectColumns));
        }

        public virtual async Task<long> GetLengthAsync(CancellationToken cancellationToken = default)
        {
            using (var cancellationTokenSource = CreateCancellationTokenSource())
            {
                using (var connection = await GetConnectionAsync(cancellationTokenSource.Token).ConfigureAwait(false))
                {
                    using (var countCommand = connection.CreateSelectCountCommand(DatabaseDriver, Table.Schema, Table.Name, Table.Columns, filter: null))
                    {
                        return Convert.ToInt64(
                            await countCommand.ExecuteScalarAsync(cancellationTokenSource.Token).ConfigureAwait(false));
                    }
                }
            }
        }
    }
}
