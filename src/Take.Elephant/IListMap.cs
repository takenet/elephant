using System.Threading.Tasks;

namespace Take.Elephant
{
    /// <summary>
    /// Represents a map that contains a list
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TItem"></typeparam>
    public interface IListMap<TKey, TItem> : IMap<TKey, IPositionList<TItem>>
    {
        /// <summary>
        /// Gets the value for the key if the exists or a new list for the type, if not.
        /// If the later, the item is automatically added to the map.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns>An existing list if the key exists; otherwise, an empty list.</returns>
        Task<IPositionList<TItem>> GetValueOrEmptyAsync(TKey key);
    }
}