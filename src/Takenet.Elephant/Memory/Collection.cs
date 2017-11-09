using System;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace Takenet.Elephant.Memory
{
    public abstract class Collection<T> : ICollection<T>, IQueryableStorage<T>, IOrderedQueryableStorage<T>
    {
        private readonly System.Collections.Generic.ICollection<T> _collection;

        protected Collection(System.Collections.Generic.ICollection<T> collection)
        {
            _collection = collection;
        }

        public virtual Task<IAsyncEnumerable<T>> AsEnumerableAsync()
        {
            return Task.FromResult<IAsyncEnumerable<T>>(new AsyncEnumerableWrapper<T>(_collection));
        }

        public virtual Task<long> GetLengthAsync()
        {
            return Task.FromResult<long>(_collection.Count);
        }

        #region IQueryableStorage<T> Members

        public virtual Task<QueryResult<T>> QueryAsync<TResult>(Expression<Func<T, bool>> where,
            Expression<Func<T, TResult>> select,
            int skip,
            int take,
            CancellationToken cancellationToken)
        {
            if (@where == null) @where = i => true;
            var predicate = where.Compile();
            var totalValues = _collection.Where(predicate.Invoke);
            var resultValues = totalValues
                .Skip(skip)
                .Take(take)
                .ToArray();

            var result = new QueryResult<T>(new AsyncEnumerableWrapper<T>(resultValues), totalValues.Count());
            return Task.FromResult(result);
        }

        #endregion

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

            var result = new QueryResult<T>(new AsyncEnumerableWrapper<T>(resultValues), totalValues.Count());
            return Task.FromResult(result);
        }
    }
}
