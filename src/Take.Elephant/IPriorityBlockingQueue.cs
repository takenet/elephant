using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant
{
    /// <summary>
    /// Defines a queue with score that allows to await for messages when dequeueing.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IPriorityBlockingQueue<T> : IPriorityQueue<T>
    {
        /// <summary>
        /// Dequeues an item with the lowest score from the queue, awaiting for a new value if the queue is empty.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns></returns>
        Task<T> DequeueMinAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Dequeues an item with the highest score from the queue, awaiting for a new value if the queue is empty.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns></returns>
        Task<T> DequeueMaxAsync(CancellationToken cancellationToken);
    }
}
