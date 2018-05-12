using System.Threading.Tasks;

namespace Take.Elephant
{
    /// <summary>
    /// Defines a map that supports value updates under specific conditions.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    public interface IUpdatableMap<TKey, TValue> : IMap<TKey, TValue>
    {
        /// <summary>
        /// Updates the value of the key only if the existing value in the specified key equals to the specified old value.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="newValue">The new value.</param> 
        /// <param name="oldValue">The comparison value for update.</param>
        /// <returns></returns>
        Task<bool> TryUpdateAsync(TKey key, TValue newValue, TValue oldValue);
    }
}
