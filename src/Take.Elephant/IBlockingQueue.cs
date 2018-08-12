using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant
{
    /// <summary>
    /// Defines a queue that allows to await for messages when dequeueing.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IBlockingQueue<T> : IQueue<T>
    {
        /// <summary>
        /// Dequeues a value from the queue, awaiting for a new value if the queue is empty.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns></returns>
        Task<T> DequeueAsync(CancellationToken cancellationToken);
    }
}
