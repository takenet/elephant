using System;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence
{
    /// <summary>
    /// Defines a storage that supports queries for its keys.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public interface IKeyQueryableStorage<TKey, TValue> : IMap<TKey, TValue>
    {
        Task<QueryResult<TKey>> QueryForKeysAsync<TResult>(Expression<Func<TValue, bool>> where, Expression<Func<TKey, TResult>> select, int skip, int take, CancellationToken cancellationToken);
    }
}