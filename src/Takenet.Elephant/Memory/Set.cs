using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace Takenet.Elephant.Memory
{
    /// <summary>
    /// Implements the <see cref="ISet{T}"/> interface using the <see cref="System.Collections.Generic.HashSet{T}"/> class.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class Set<T> : ISet<T>, IQueryableStorage<T>
    {
        private readonly HashSet<T> _hashSet;

        #region Constructor

        public Set()
            : this(EqualityComparer<T>.Default)
        {

        }

        public Set(IEqualityComparer<T> equalityComparer)
        {
            _hashSet = new HashSet<T>(equalityComparer);
        }

        public Set(IEnumerable<T> collection)
            : this(collection, EqualityComparer<T>.Default)
        {

        }

        public Set(IEnumerable<T> collection, IEqualityComparer<T> equalityComparer)
        {
            _hashSet = new HashSet<T>(collection, equalityComparer);
        }

        #endregion

        #region ISet<T> Members

        public Task AddAsync(T value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            if (_hashSet.Contains(value))
            {
                _hashSet.Remove(value);
            }

            _hashSet.Add(value);
            return TaskUtil.CompletedTask;
        }

        public Task<bool> TryRemoveAsync(T value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            return Task.FromResult(_hashSet.Remove(value));
        }

        public Task<IAsyncEnumerable<T>> AsEnumerableAsync()
        {            
            return Task.FromResult<IAsyncEnumerable<T>>(new AsyncEnumerableWrapper<T>(_hashSet));
        }

        public Task<bool> ContainsAsync(T value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            return Task.FromResult(_hashSet.Contains(value));
        }

        #endregion

        #region IQueryableStorage<T> Members

        public Task<QueryResult<T>> QueryAsync<TResult>(Expression<Func<T, bool>> where,
            Expression<Func<T, TResult>> select,
            int skip,
            int take,
            CancellationToken cancellationToken)
        {
            var predicate = where.Compile();

            var totalValues = _hashSet
                .Where(predicate.Invoke);

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
