using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Takenet.Elephant.Memory
{
    public abstract class Collection<T> : ICollection<T>, IQueryableStorage<T>
    {
        private readonly System.Collections.Generic.ICollection<T> _collection;

        protected Collection(System.Collections.Generic.ICollection<T> collection)
        {
            _collection = collection;
        }

        public Task<IAsyncEnumerable<T>> AsEnumerableAsync()
        {
            return Task.FromResult<IAsyncEnumerable<T>>(new AsyncEnumerableWrapper<T>(_collection));
        }

        public Task<long> GetLengthAsync()
        {
            return Task.FromResult<long>(_collection.Count);
        }

        #region IQueryableStorage<T> Members

        public Task<QueryResult<T>> QueryAsync<TResult>(Expression<Func<T, bool>> where,
            Expression<Func<T, TResult>> select,
            int skip,
            int take,
            CancellationToken cancellationToken)
        {
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
    }
}
