using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence
{
    /// <summary>
    /// Defines a storage that supports queries.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IQueryableStorage<T>
    {
        /// <summary>
        /// Queries the storage
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="where"></param>
        /// <param name="select"></param>
        /// <param name="skip"></param>
        /// <param name="take"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task<QueryResult<T>> QueryAsync<TResult>(Expression<Func<T, bool>> where, Expression<Func<T, TResult>> select, int skip, int take, CancellationToken cancellationToken);
    }

    /// <summary>
    /// Defines a storage that supports queries which returns keys.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public interface IQueryableStorage<TKey, TValue>
    {
        Task<QueryResult<TKey>> QueryForKeysAsync<TResult>(Expression<Func<TValue, bool>> where, Expression<Func<TKey, TResult>> select, int skip, int take, CancellationToken cancellationToken);
    }

    public sealed class QueryResult<T> : IEnumerable<T>
    {
        public QueryResult(IEnumerable<T> items, int total)
        {
            Items = items;
            Total = total;
        }

        public IEnumerable<T> Items { get; }

        public int Total { get; private set; }

        #region IEnumerable<T> Members

        public IEnumerator<T> GetEnumerator()
        {
            return Items.GetEnumerator();
        }

        #endregion

        #region IEnumerable Members

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable)Items).GetEnumerator();
        }

        #endregion
    }
}
