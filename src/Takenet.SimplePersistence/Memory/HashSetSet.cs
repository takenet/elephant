using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence.Memory
{
    /// <summary>
    /// Implements the <see cref="ISet{T}"/> interface with the <see cref="HashSet{T}"/> class.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class HashSetSet<T> : ISet<T> //, IQueryableStorage<T>
    {
        private readonly HashSet<T> _hashSet;

        public HashSetSet()
        {
            _hashSet = new HashSet<T>();
        }

        #region ISet<T> Members

        public Task AddAsync(T value)
        {
            if (_hashSet.Contains(value))
            {
                _hashSet.Remove(value);
            }

            _hashSet.Add(value);
            return TaskUtil.CompletedTask;
        }

        public Task<bool> TryRemoveAsync(T value)
        {
            return Task.FromResult(_hashSet.Remove(value));
        }

        public Task<IEnumerable<T>> AsEnumerableAsync()
        {
            return Task.FromResult<IEnumerable<T>>(_hashSet);
        }

        public Task<bool> ContainsAsync(T value)
        {
            return Task.FromResult(_hashSet.Contains(value));
        }

        #endregion

        #region IQueryableStorage<T> Members

        //public Task<QueryResult<T>> QueryAsync<TResult>(Expression<Func<T, bool>> where,
        //    Expression<Func<T, TResult>> select,
        //    int skip,
        //    int take,
        //    CancellationToken cancellationToken)
        //{
        //    var predicate = where.Compile();

        //    var totalValues = this._hashSet
        //        .Where(predicate.Invoke);

        //    var resultValues = totalValues
        //        .Skip(skip)
        //        .Take(take)
        //        .ToArray();

        //    var result = new QueryResult<T>(resultValues, totalValues.Count());

        //    return Task.FromResult(result);
        //}

        #endregion
    }
}
