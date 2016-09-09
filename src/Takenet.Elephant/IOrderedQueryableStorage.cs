using System;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace Takenet.Elephant
{
    /// <summary>
    /// Defines a storage that supports ordered queries.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IOrderedQueryableStorage<T>
    {
        /// <summary>
        /// Submits a query into the storage container.
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <typeparam name="TOrderBy"></typeparam>
        /// <param name="where"></param>
        /// <param name="select"></param>
        /// <param name="orderBy"></param>
        /// <param name="orderByAscending"></param>
        /// <param name="skip"></param>
        /// <param name="take"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task<QueryResult<T>> QueryAsync<TResult, TOrderBy>(Expression<Func<T, bool>> where, Expression<Func<T, TResult>> select, Expression<Func<T, TOrderBy>> orderBy, bool orderByAscending, int skip, int take, CancellationToken cancellationToken);
    }
}