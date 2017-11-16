using System.Threading.Tasks;

namespace Takenet.Elephant
{
    public static class QueueMapExtensions
    {
        /// <summary>
        /// Adds an item to the queue.
        /// If the key doesn't exists, it will be created.
        /// If the value already exists, it is overwritten.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <typeparam name="TItem">The type of the item.</typeparam>
        /// <param name="queueMap">The queue map.</param>
        /// <param name="key">The key.</param>
        /// <param name="item">The item.</param>
        /// <returns></returns>
        public static async Task EnqueueItemAsync<TKey, TItem>(this IQueueMap<TKey, TItem> queueMap, TKey key, TItem item)
        {
            var queue = await queueMap.GetValueOrEmptyAsync(key).ConfigureAwait(false);
            await queue.EnqueueAsync(item).ConfigureAwait(false);
        }

        /// <summary>
        /// Dequeue an item in the queue on the specified key.
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TItem"></typeparam>
        /// <param name="queueMap"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        public static async Task<TItem> DequeueItemOrDefaultAsync<TKey, TItem>(this IQueueMap<TKey, TItem> queueMap,
            TKey key)
        {
            var queue = await queueMap.GetValueOrDefaultAsync(key).ConfigureAwait(false);
            if (queue == null) return default(TItem);
            return await queue.DequeueOrDefaultAsync().ConfigureAwait(false);
        }
    }
}