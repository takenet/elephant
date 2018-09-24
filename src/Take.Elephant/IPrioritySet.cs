using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant
{
    /// <summary>
    /// Defines a FIFO storafe container with score
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IPrioritySet<T> : IPriorityReceiverSet<T>, IPrioritySenderSet<T>
    { }

    public interface IPriorityReceiverSet<T>
    {
        /// <summary>
        /// Dequeues an item with the lowest score from the queue, if available.
        /// </summary>
        /// <returns></returns>
        Task<T> RemoveMinOrDefaultAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Dequeues an item with the highest score from the queue, if available.
        /// </summary>
        /// <returns></returns>
        Task<T> RemoveMaxOrDefaultAsync(CancellationToken cancellationToken = default);
    }

    public interface IPrioritySenderSet<T>
    {
        /// <summary>
        /// Enqueues an item with score.
        /// </summary>
        /// <param name="item"></param>
        /// <param name="score"></param>
        /// <returns></returns>
        Task AddAsync(T item, double score, CancellationToken cancellationToken = default);
    }
}