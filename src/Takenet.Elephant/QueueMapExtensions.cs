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
    }
}