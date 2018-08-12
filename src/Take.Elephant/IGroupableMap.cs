using System;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant
{
    /// <summary>
    /// Defines a <see cref="IMap{TKey, TValue}"/> that support grouping queries.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public interface IGroupableMap<TKey, TValue> : IMap<TKey, TValue>
    {
        /// <summary>
        /// Gets the groups of <see cref="TValue"/> using the specified property name as group key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="propertyName"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task<Group<TValue>[]> GroupByPropertyAsync(TKey key, string propertyName, CancellationToken cancellationToken = default);
    }
}