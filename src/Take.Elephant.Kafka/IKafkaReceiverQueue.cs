using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Kafka
{
    /// <summary>
    /// Optional Kafka receiver contract that allows reading message headers together with the payload.
    /// </summary>
    /// <typeparam name="T">Payload type.</typeparam>
    public interface IKafkaReceiverQueue<T> : IReceiverQueue<T>, IBlockingReceiverQueue<T>
    {
        /// <summary>
        /// Dequeues a payload with its Kafka headers, if available.
        /// </summary>
        Task<KafkaConsumedMessage<T>> DequeueWithHeadersOrDefaultAsync(
            CancellationToken cancellationToken = default
        );

        /// <summary>
        /// Dequeues a payload with its Kafka headers, awaiting for a message if needed.
        /// </summary>
        Task<KafkaConsumedMessage<T>> DequeueWithHeadersAsync(
            CancellationToken cancellationToken
        );
    }
}