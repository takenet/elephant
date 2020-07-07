using System;
using System.Threading.Tasks;

namespace Take.Elephant
{
    /// <summary>
    /// Defines a set that supports item expiration.
    /// </summary>
    /// <typeparam name="T">The type of the item.</typeparam>
    public interface IExpirableItem<T> : ISet<T>
    {
        /// <summary>
        /// Sets the relative item expiration date.
        /// </summary>
        /// <param name="item">The item.</param>
        /// <param name="ttl">The TTL.</param>
        /// <returns></returns>
        Task<bool> SetRelativeItemExpirationAsync(T item, TimeSpan ttl);

        /// <summary>
        /// Sets the absolute key expiration date.
        /// </summary>
        /// <param name="item">The item.</param>
        /// <param name="expiration">The expiration.</param>
        /// <returns></returns>
        Task<bool> SetAbsoluteItemExpirationAsync(T item, DateTimeOffset expiration);

        /// <summary>
        /// Remove expiration value from an item register
        /// </summary>
        /// <param name="item">The item.</param>
        /// <returns></returns>
        Task<bool> RemoveExpirationAsync(T item);
    }
}
