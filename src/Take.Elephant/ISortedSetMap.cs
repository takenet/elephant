using System.Threading.Tasks;

namespace Take.Elephant
{
    /// <summary>
    /// Represents a map that contains a sorted set
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TItem"></typeparam>
    public interface ISortedSetMap<TKey, TItem> : IMap<TKey, ISortedSet<TItem>>
    {
        /// <summary>
        /// Gets the value for the key if the exists or a new sorted set for the type, if not.
        /// If the later, the item is automatically added to the map.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns>An existing sorted set if the key exists; otherwise, an empty sorted set.</returns>
        Task<ISortedSet<TItem>> GetValueOrEmptyAsync(TKey key);
    }
}