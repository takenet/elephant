using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence
{
    /// <summary>
    /// Defines a map that supports key expiration.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    public interface IExpirableKeyMap<TKey, TValue> : IMap<TKey, TValue>
    {
        /// <summary>
        /// Sets the relative key expiration date.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="ttl">The TTL.</param>
        /// <returns></returns>
        Task SetRelativeKeyExpirationAsync(TKey key, TimeSpan ttl);

        /// <summary>
        /// Sets the absolute key expiration date.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="expiration">The expiration.</param>
        /// <returns></returns>
        Task SetAbsoluteKeyExpirationAsync(TKey key, DateTimeOffset expiration);
    }
}
