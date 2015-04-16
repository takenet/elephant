using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence
{
    /// <summary>
    /// Represents a map for number values with specific features.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    public interface INumberMap<TKey> : IMap<TKey, long>
    {
        /// <summary>
        /// Atomically increments the value of the key by one.
        /// If the key does not exists, it will be created with the value 0.
        /// </summary>
        /// <param name="key">The item key.</param>
        /// <returns>The number updated value.</returns>
        Task<long> IncrementAsync(TKey key);

        /// <summary>
        /// Atomically increments the value of the key.
        /// If the key does not exists, it will be created with the value 0.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <returns>The number updated value.</returns>
        Task<long> IncrementAsync(TKey key, long value);

        /// <summary>
        /// Atomically decrements the value of the key by one.
        /// If the key does not exists, it will be created with the value 0.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns>The number updated value.</returns>
        Task<long> DecrementAsync(TKey key);

        /// <summary>
        /// Atomically decrements the value of the key.
        /// If the key does not exists, it will be created with the value 0.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value"></param>
        /// <returns>The number updated value.</returns>
        Task<long> DecrementAsync(TKey key, long value);
    }
}
