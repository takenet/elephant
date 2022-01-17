using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Sql
{
    /// <summary>
    /// Implements a <see cref="SqlSet{T}"/> that uses distinct connections for read and write operations,
    /// providing the adequate "ApplicationIntent" parameter to the connection string in each case.
    /// This is useful when there is replica databases that can be used for read operations, reducing the overhead in the main instances.
    /// More info:
    /// https://docs.microsoft.com/en-us/sql/database-engine/availability-groups/windows/secondary-replica-connection-redirection-always-on-availability-groups
    /// </summary>
    public class ApplicationIntentSqlSet<T> : ApplicationIntentStorageBase, ISet<T>, IQueryableStorage<T>, IOrderedQueryableStorage<T>, IDistinctQueryableStorage<T>
    {
        private readonly SqlSet<T> _readOnlySet;
        private readonly SqlSet<T> _writeSet;
        
        public ApplicationIntentSqlSet(IDatabaseDriver databaseDriver, string connectionString, ITable table, IMapper<T> mapper)
            : base(databaseDriver, connectionString, table)
        {
            _readOnlySet = new SqlSet<T>(databaseDriver, ReadOnlyConnectionString, ReadOnlyTable, mapper);
            _writeSet = new SqlSet<T>(databaseDriver, connectionString, table, mapper);
        }
        
        public virtual async Task AddAsync(T value, CancellationToken cancellationToken = default) => await _writeSet.AddAsync(value, cancellationToken);

        public virtual async Task<bool> TryRemoveAsync(T value, CancellationToken cancellationToken = default) => await _writeSet.TryRemoveAsync(value, cancellationToken);

        public virtual async Task<bool> ContainsAsync(T value, CancellationToken cancellationToken = default)
        {
            await SynchronizeSchemaAsync(cancellationToken);
            return await _readOnlySet.ContainsAsync(value, cancellationToken);
        }

        public virtual async Task<long> GetLengthAsync(CancellationToken cancellationToken = default)
        {
            await SynchronizeSchemaAsync(cancellationToken);
            return await _readOnlySet.GetLengthAsync(cancellationToken);
        }

        public virtual async IAsyncEnumerable<T> AsEnumerableAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await SynchronizeSchemaAsync(cancellationToken);
            await foreach (var item in _readOnlySet.AsEnumerableAsync(cancellationToken))
            {
                yield return item;
            }
        }

        public virtual async Task<QueryResult<T>> QueryAsync<TResult>(Expression<Func<T, bool>> @where, Expression<Func<T, TResult>> @select, int skip, int take,
            CancellationToken cancellationToken)
        {
            await SynchronizeSchemaAsync(cancellationToken);
            return await _readOnlySet.QueryAsync(@where, @select, skip, take, cancellationToken);
        }

        public virtual async Task<QueryResult<T>> QueryAsync<TResult, TOrderBy>(Expression<Func<T, bool>> @where, Expression<Func<T, TResult>> @select, Expression<Func<T, TOrderBy>> orderBy, bool orderByAscending,
            int skip, int take, CancellationToken cancellationToken)
        {
            await SynchronizeSchemaAsync(cancellationToken);
            return await _readOnlySet.QueryAsync(@where, @select, orderBy, orderByAscending, skip, take, cancellationToken);
        }

        public virtual async Task<QueryResult<T>> QueryAsync<TResult>(Expression<Func<T, bool>> @where, Expression<Func<T, TResult>> @select, bool distinct, int skip, int take,
            CancellationToken cancellationToken)
        {
            await SynchronizeSchemaAsync(cancellationToken);
            return await _readOnlySet.QueryAsync(@where, @select, distinct, skip, take, cancellationToken);
        }
    }
}