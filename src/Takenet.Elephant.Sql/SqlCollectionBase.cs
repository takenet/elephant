using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Takenet.Elephant.Sql.Mapping;

namespace Takenet.Elephant.Sql
{
    public abstract class SqlCollectionBase<T> : StorageBase<T>, ICollection<T>
    {        
        protected SqlCollectionBase(IDatabaseDriver databaseDriver, string connectionString, ITable table, IMapper<T> mapper)
            : base(databaseDriver, connectionString, table, mapper)
        {

        }

        public virtual Task<IAsyncEnumerable<T>> AsEnumerableAsync()
        {
            var selectColumns = Table.Columns.Keys.ToArray();
            return Task.FromResult<IAsyncEnumerable<T>>(
                new DbDataReaderAsyncEnumerable<T>(
                    GetConnectionAsync,
                    c => c.CreateSelectCommand(DatabaseDriver, Table.Name, null, selectColumns),
                    Mapper,
                    selectColumns));
        }

        public virtual async Task<long> GetLengthAsync()
        {
            using (var cancellationTokenSource = CreateCancellationTokenSource())
            {
                using (var connection = await GetConnectionAsync(cancellationTokenSource.Token).ConfigureAwait(false))
                {
                    using (var countCommand = connection.CreateSelectCountCommand(DatabaseDriver, Table.Name, filter: null))
                    {
                        return Convert.ToInt64(
                            await countCommand.ExecuteScalarAsync(cancellationTokenSource.Token).ConfigureAwait(false));
                    }
                }
            }
        }
    }
}
