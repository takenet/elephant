using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Kafka
{
    /// <summary>
    /// Extended Kafka receiver contract that exposes ackable messages.
    /// Consumers that require acknowledgement control (OnSuccess or Manual modes)
    /// should use <see cref="DequeueAckableAsync"/> or <see cref="DequeueAckableOrDefaultAsync"/>.
    /// </summary>
    /// <typeparam name="T">Payload type.</typeparam>
    public interface IKafkaAckableReceiverQueue<T> : IKafkaReceiverQueue<T>
    {
        /// <summary>
        /// Dequeues an ackable message, awaiting for a new value if the queue is empty.
        /// The returned <see cref="KafkaAckableMessage{T}"/> must be acknowledged after
        /// successful processing.
        /// </summary>
        Task<KafkaAckableMessage<T>> DequeueAckableAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Dequeues an ackable message if available; returns <see langword="null"/> otherwise.
        /// </summary>
        Task<KafkaAckableMessage<T>> DequeueAckableOrDefaultAsync(CancellationToken cancellationToken = default);
    }
}
