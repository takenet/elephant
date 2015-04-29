using System;
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
        /// Submits a query into the storage container.
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
}
