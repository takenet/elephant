using System.Threading.Tasks;

namespace Takenet.Elephant
{
    /// <summary>
    /// Defines a set of unique items.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface ISet<T> : ICollection<T>
    {
        /// <summary>
        /// Adds an item to the set. 
        /// If the value already exists, it is overwritten.
        /// </summary>
        /// <param name="value">The value.</param>        
        /// <returns></returns>
        Task AddAsync(T value);

        /// <summary>
        /// Tries to remove an existing item from the set.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        Task<bool> TryRemoveAsync(T value);

        /// <summary>
        /// Checks if the value exists in the set.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        Task<bool> ContainsAsync(T value);
    }
}
