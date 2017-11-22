using System;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace Takenet.Elephant
{
    /// <summary>
    /// Defines a storage that supports queries for distinct values.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IDistinctQueryableStorage<T>
    {
        /// <summary>
        /// Submits a query into the storage container.
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="where"></param>
        /// <param name="select"></param>
        /// <param name="distinct"></param>
        /// <param name="skip"></param>
        /// <param name="take"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task<QueryResult<T>> QueryAsync<TResult>(Expression<Func<T, bool>> where, Expression<Func<T, TResult>> select, bool distinct, int skip, int take, CancellationToken cancellationToken);
    }
}