using System.Threading.Tasks;

namespace Take.Elephant.Memory
{
    /// <summary>
    /// Implementats the <see cref="IListMap{TKey, TValue}"/>
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public class ListMap<TKey, TValue> : Map<TKey, IPositionList<TValue>>, IListMap<TKey, TValue>
    {
        public ListMap()
            : base(() => new List<TValue>())
        {
        }

        /// <summary>
        /// Gets the value for the key if the exists or a new list for the type, if not.
        /// If the later, the item is automatically added to the map.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns>An existing list if the key exists; otherwise, an empty list.</returns>
        public Task<IPositionList<TValue>> GetValueOrEmptyAsync(TKey key)
            => InternalDictionary.GetOrAdd(key, k => ValueFactory()).AsCompletedTask();
    }
}