using System;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant
{
    /// <summary>
    /// Defines a map that supports key expiration.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    public interface IExpirableKeyMap<TKey, TValue> : IMap<TKey, TValue>
    {
        /// <summary>
        /// Tries to add an item with expiration relative to the current time.
        /// </summary>
        /// <param name="key">The item key</param>
        /// <param name="value">The value.</param>
        /// <param name="expiration">The relative expiration time span.</param>
        /// <param name="overwrite">Indicates if the item should be overwritten if the key already exists.</param>
        /// <param name="cancellationToken"></param>
        /// <returns><see langword="true"/> if the item was added; <see langword="false"/> otherwise.</returns>
        Task<bool> TryAddWithRelativeExpirationAsync(TKey key, TValue value,
            TimeSpan expiration = default,
            bool overwrite = false, CancellationToken cancellationToken = default);

        /// <summary>
        /// Tries to add an item with absolute expiration date time
        /// </summary>
        /// <param name="key">The item key</param>
        /// <param name="value">The value.</param>
        /// <param name="expiration">The absolute expiration date.</param>
        /// <param name="overwrite">Indicates if the item should be overwritten if the key already exists.</param>
        /// <param name="cancellationToken"></param>
        /// <returns><see langword="true"/> if the item was added; <see langword="false"/> otherwise.</returns>
        Task<bool> TryAddWithAbsoluteExpirationAsync(TKey key, TValue value,
            DateTimeOffset expiration = default, bool overwrite = false,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Sets the relative key expiration date.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="ttl">The TTL.</param>
        /// <returns></returns>
        Task<bool> SetRelativeKeyExpirationAsync(TKey key, TimeSpan ttl);

        /// <summary>
        /// Sets the absolute key expiration date.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="expiration">The expiration.</param>
        /// <returns></returns>
        Task<bool> SetAbsoluteKeyExpirationAsync(TKey key, DateTimeOffset expiration);

        /// <summary>
        /// Remove expiration value from a register
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns></returns>
        Task<bool> RemoveExpirationAsync(TKey key);
    }
}