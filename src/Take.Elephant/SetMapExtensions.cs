using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant
{
    public static class SetMapExtensions
    {
        /// <summary>
        /// Adds an item to the set.
        /// If the key doesn't exists, it will be created.
        /// If the value already exists, it is overwritten.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <typeparam name="TItem">The type of the item.</typeparam>
        /// <param name="setMap">The set map.</param>
        /// <param name="key">The key.</param>
        /// <param name="item">The item.</param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static async Task AddItemAsync<TKey, TItem>(this ISetMap<TKey, TItem> setMap,
            TKey key,
            TItem item,
            CancellationToken cancellationToken = default)
        {
            var set = await setMap.GetValueOrEmptyAsync(key, cancellationToken).ConfigureAwait(false);
            await set.AddAsync(item, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Tries to remove an existing item from the set.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <typeparam name="TItem">The type of the item.</typeparam>
        /// <param name="setMap">The set map.</param>
        /// <param name="key">The key.</param>
        /// <param name="item">The item.</param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static async Task<bool> TryRemoveItemAsync<TKey, TItem>(this ISetMap<TKey, TItem> setMap,
            TKey key,
            TItem item,
            CancellationToken cancellationToken = default)
        {
            var set = await setMap.GetValueOrEmptyAsync(key, cancellationToken).ConfigureAwait(false);
            return await set.TryRemoveAsync(item, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Checks if the key exists and the value exists in the set.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <typeparam name="TItem">The type of the item.</typeparam>
        /// <param name="setMap">The set map.</param>
        /// <param name="key">The key.</param>
        /// <param name="item">The item.</param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static async Task<bool> ContainsItemAsync<TKey, TItem>(this ISetMap<TKey, TItem> setMap,
            TKey key,
            TItem item,
            CancellationToken cancellationToken = default)
        {
            var set = await setMap.GetValueOrEmptyAsync(key, cancellationToken).ConfigureAwait(false);
            return await set.ContainsAsync(item, cancellationToken).ConfigureAwait(false);
        }
    }
}