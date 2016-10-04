using System.Threading.Tasks;

namespace Takenet.Elephant
{
    /// <summary>
    /// Represents a map that contains a set on unique items.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TItem"></typeparam>
    public interface ISetMap<TKey, TItem> : IMap<TKey, ISet<TItem>>
    {
        /// <summary>
        /// Gets the value for the key if the exists or a new set for the type, if not.
        /// If the later, the item is automatically added to the map.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns>An existing set if the key exists; otherwise, an empty set.</returns>
        Task<ISet<TItem>> GetValueOrEmptyAsync(TKey key);
    }
}
