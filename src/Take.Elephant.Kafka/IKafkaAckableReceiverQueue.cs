using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Kafka
{
    /// <summary>
    /// Kafka receiver contract that exposes ackable messages for
    /// <see cref="KafkaAckMode.OnSuccess"/> and <see cref="KafkaAckMode.Manual"/> modes.
    /// </summary>
    /// <remarks>
    /// This interface is intentionally <b>not</b> derived from
    /// <see cref="IKafkaReceiverQueue{T}"/> because <see cref="KafkaReceiverQueue{T}"/>
    /// throws <see cref="System.InvalidOperationException"/> from the
    /// <c>DequeueAsync</c> / <c>DequeueOrDefault*</c> / <c>DequeueWithHeaders*</c> members
    /// in non-Eager modes. Deriving from the base interface would violate the
    /// Liskov Substitution Principle: code typed against <see cref="IKafkaReceiverQueue{T}"/>
    /// could receive an <see cref="IKafkaAckableReceiverQueue{T}"/> and fail at runtime.
    /// <para>
    /// <see cref="KafkaReceiverQueue{T}"/> implements both interfaces with publicly accessible
    /// methods. Declare your dependency as <see cref="IKafkaReceiverQueue{T}"/> for Eager mode,
    /// or as <see cref="IKafkaAckableReceiverQueue{T}"/> for OnSuccess / Manual modes.
    /// Prefer the <see cref="KafkaReceiverQueue"/> static factory class, which returns the
    /// narrowest interface for the requested mode and makes accidental misregistration a
    /// compile-time error instead of a runtime failure.
    /// </para>
    /// </remarks>
    /// <typeparam name="T">Payload type.</typeparam>
    public interface IKafkaAckableReceiverQueue<T>
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
