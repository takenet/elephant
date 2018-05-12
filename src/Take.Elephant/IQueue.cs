using System.Threading.Tasks;

namespace Take.Elephant
{
    /// <summary>
    /// Defines a FIFO storage container.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IQueue<T> : IReceiverQueue<T>, ISenderQueue<T>
    {
        /// <summary>
        /// Gets the number of items in the queue.
        /// </summary>
        /// <returns></returns>
        Task<long> GetLengthAsync();
    }

    public interface IReceiverQueue<T>
    {
        /// <summary>
        /// Dequeues an item from the queue, if available.
        /// </summary>
        /// <returns></returns>
        Task<T> DequeueOrDefaultAsync();
    }

    public interface ISenderQueue<T>
    {
        /// <summary>
        /// Enqueues an item.
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        Task EnqueueAsync(T item);
    }        
}
