using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Kafka
{
    /// <summary>
    /// Kafka receiver contract for <see cref="KafkaAckMode.OnSuccess"/> and <see cref="KafkaAckMode.Manual"/> modes.
    /// Intentionally not derived from <see cref="IKafkaReceiverQueue{T}"/> — non-Eager implementations
    /// throw on the legacy Dequeue* methods, which would violate LSP.
    /// </summary>
    /// <typeparam name="T">Payload type.</typeparam>
    public interface IKafkaAckableReceiverQueue<T>
    {
        /// <summary>Dequeues an ackable message, waiting if the queue is empty. Must be acknowledged after processing.</summary>
        Task<KafkaAckableMessage<T>> DequeueAckableAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Dequeues an ackable message if available; returns <see langword="null"/> otherwise.
        /// </summary>
        Task<KafkaAckableMessage<T>> DequeueAckableOrDefaultAsync(CancellationToken cancellationToken = default);
    }
}
