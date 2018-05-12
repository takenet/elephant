using System.Threading.Tasks;

namespace Take.Elephant
{
    /// <summary>
    /// Defines a FIFO storage container.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IQueue<T>
    {
        /// <summary>
        /// Enqueues an item.
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        Task EnqueueAsync(T item);

        /// <summary>
        /// Dequeues an item from the queue, if available.
        /// </summary>
        /// <returns></returns>
        Task<T> DequeueOrDefaultAsync();

        /// <summary>
        /// Gets the number of items in the queue.
        /// </summary>
        /// <returns></returns>
        Task<long> GetLengthAsync();
    }
}
