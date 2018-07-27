using System.Threading.Tasks;

namespace Take.Elephant
{
    /// <summary>
    /// Defines a FIFO storafe container with score
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IPriorityQueue<T> : IPriorityReceiverQueue<T>, IPrioritySenderQueue<T>
    { }

    public interface IPriorityReceiverQueue<T>
    {
        /// <summary>
        /// Dequeues an item with the lowest score from the queue, if available.
        /// </summary>
        /// <returns></returns>
        Task<T> DequeueMinOrDefaultAsync();

        /// <summary>
        /// Dequeues an item with the highest score from the queue, if available.
        /// </summary>
        /// <returns></returns>
        Task<T> DequeueMaxOrDefaultAsync();
    }

    public interface IPrioritySenderQueue<T>
    {
        /// <summary>
        /// Enqueues an item with score.
        /// </summary>
        /// <param name="item"></param>
        /// <param name="score"></param>
        /// <returns></returns>
        Task EnqueueAsync(T item, float score);
    }
}
