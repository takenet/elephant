using System.Collections.Generic;
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
        public static async Task AddItemAsync<TKey, TItem>(
            this ISetMap<TKey, TItem> setMap,
            TKey key,
            TItem item,
            CancellationToken cancellationToken = default)
        {
            var set = await setMap.GetValueOrEmptyAsync(key, cancellationToken).ConfigureAwait(false);
            await set.AddAsync(item, cancellationToken).ConfigureAwait(false);
        }
        
        /// <summary>
        /// Adds some items to the set.
        /// If the key doesn't exists, it will be created.
        /// If the value already exists, it is overwritten.
        /// </summary>
        public static async Task AddItemsAsync<TKey, TItem>(
            this ISetMap<TKey, TItem> setMap,
            TKey key,
            IAsyncEnumerable<TItem> items,
            CancellationToken cancellationToken = default)
        {
            var set = await setMap.GetValueOrEmptyAsync(key, cancellationToken).ConfigureAwait(false);
            await set.AddRangeAsync(items, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Tries to remove an existing item from the set.
        /// </summary>
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