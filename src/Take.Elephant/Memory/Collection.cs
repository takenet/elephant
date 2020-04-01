using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Memory
{
    public abstract class Collection<T> : ICollection<T>, IQueryableStorage<T>, IOrderedQueryableStorage<T>, IDistinctQueryableStorage<T>
    {
        private readonly System.Collections.Generic.ICollection<T> _collection;

        protected Collection(System.Collections.Generic.ICollection<T> collection)
        {
            _collection = collection;
        }

        /// <summary>
        /// Enable/disable fetching of total record count on Queries.
        /// Default: Enabled.
        /// </summary>
        public bool FetchQueryResultTotal { get; set; } = true;

        public virtual IAsyncEnumerable<T> AsEnumerableAsync(CancellationToken cancellationToken = default) => new AsyncEnumerableWrapper<T>(_collection);

        public virtual Task<long> GetLengthAsync(CancellationToken cancellationToken = default) => Task.FromResult<long>(_collection.Count);

        public virtual Task<QueryResult<T>> QueryAsync<TResult>(Expression<Func<T, bool>> where,
            Expression<Func<T, TResult>> select,
            int skip,
            int take,
            CancellationToken cancellationToken) => QueryAsync<TResult>(@where, @select, false, skip, take, cancellationToken);

        public virtual Task<QueryResult<T>> QueryAsync<TResult>(Expression<Func<T, bool>> @where, Expression<Func<T, TResult>> @select, bool distinct, int skip, int take,
            CancellationToken cancellationToken)
        {            
            if (@where == null) @where = i => true;
            var predicate = where.Compile();
            var totalValues = _collection.Where(predicate.Invoke);
            if (distinct) totalValues = totalValues.Distinct();

            var resultValues = totalValues
                .Skip(skip)
                .Take(take)
                .ToArray();

            int totalCount = 0;
            if (FetchQueryResultTotal)
            {
                totalCount = totalValues.Count();
            }

            var result = new QueryResult<T>(new AsyncEnumerableWrapper<T>(resultValues), totalCount);
            return Task.FromResult(result);
        }

        public virtual Task<QueryResult<T>> QueryAsync<TResult, TOrderBy>(Expression<Func<T, bool>> @where, Expression<Func<T, TResult>> @select, Expression<Func<T, TOrderBy>> orderBy, bool orderByAscending,
            int skip, int take, CancellationToken cancellationToken)
        {
            if (@where == null) @where = i => true;
            var predicate = where.Compile();
            var totalValues = _collection.Where(predicate.Invoke);

            var orderByFunc = orderBy.Compile();
            IOrderedEnumerable<T> orderedTotalValues;
            if (orderByAscending)
            {
                orderedTotalValues = totalValues.OrderBy(orderByFunc.Invoke);
            }
            else
            {
                orderedTotalValues = totalValues.OrderByDescending(orderByFunc.Invoke);
            }            

            var resultValues = orderedTotalValues
                .Skip(skip)
                .Take(take)
                .ToArray();

            int totalCount = 0;
            if (FetchQueryResultTotal)
            {
                totalCount = totalValues.Count();
            }

            var result = new QueryResult<T>(new AsyncEnumerableWrapper<T>(resultValues), totalCount);
            return Task.FromResult(result);
        }
    }
}
