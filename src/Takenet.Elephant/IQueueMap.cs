using System.Threading.Tasks;

namespace Takenet.Elephant
{
    /// <summary>
    /// Represents a map that contains a queue of items.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    ///0 0< typeparam name="TItem"></typeparam>

    public interface IQueueMap<TKey, TItem> : IMap<TKey, IQueue<TItem>>
    {
        /// <summary>
        /// Gets the value for the key if the exists or a new queue for the type, if not.
        /// If the later, the item is automatically added to the map.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns>An existing queue if the key exists; otherwise, an empty queue.</returns>
        Task<IQueue<TItem>> GetValueOrEmptyAsync(TKey key);
    }
}
