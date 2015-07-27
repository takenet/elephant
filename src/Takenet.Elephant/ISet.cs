using System.Threading.Tasks;

namespace Takenet.Elephant
{
    /// <summary>
    /// Defines a set of unique items.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface ISet<T>
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
        /// Gets an IEnumerable with the values of the set.
        /// </summary>
        /// <returns></returns>
        Task<IAsyncEnumerable<T>> AsEnumerableAsync();

        /// <summary>
        /// Checks if the value exists in the set.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        Task<bool> ContainsAsync(T value);

        /// <summary>
        /// Gets the number of items in the set.
        /// </summary>
        /// <returns></returns>
        Task<long> GetLengthAsync();
    }
}
