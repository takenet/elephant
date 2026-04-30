using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Kafka
{
    /// <summary>
    /// A Kafka message that carries the payload, headers, and a delegate to acknowledge the offset.
    /// The acknowledge operation commits the offset to the broker immediately via synchronous
    /// <c>Commit(IEnumerable&lt;TopicPartitionOffset&gt;)</c>, bypassing the local offset store.
    /// Call <see cref="AcknowledgeAsync"/> only after the business processing is fully completed.
    /// </summary>
    /// <typeparam name="T">Payload type.</typeparam>
    public sealed class KafkaAckableMessage<T>
    {
        private readonly Func<CancellationToken, Task> _acknowledge;
        private int _acknowledged;

        public KafkaAckableMessage(
            T item,
            IReadOnlyDictionary<string, byte[]> headers,
            string topic,
            int partition,
            long offset,
            Func<CancellationToken, Task> acknowledge)
        {
            Item = item;
            Headers = headers ?? KafkaConsumedMessageDefaults.EmptyHeaders;
            Topic = topic;
            Partition = partition;
            Offset = offset;
            _acknowledge = acknowledge ?? throw new ArgumentNullException(nameof(acknowledge));
        }

        /// <summary>The deserialized payload.</summary>
        public T Item { get; }

        /// <summary>Kafka message headers.</summary>
        public IReadOnlyDictionary<string, byte[]> Headers { get; }

        /// <summary>Topic the message was consumed from.</summary>
        public string Topic { get; }

        /// <summary>Partition the message was consumed from.</summary>
        public int Partition { get; }

        /// <summary>Offset of the message in the partition.</summary>
        public long Offset { get; }

        /// <summary>Whether this message has already been acknowledged.</summary>
        public bool IsAcknowledged => _acknowledged == 1;

        /// <summary>
        /// Acknowledges the message by committing its offset to the broker.
        /// Idempotent: once the commit succeeds, subsequent calls are no-ops.
        /// If the commit throws, <see cref="IsAcknowledged"/> remains <see langword="false"/>
        /// so the caller may retry by calling this method again.
        /// </summary>
        public Task AcknowledgeAsync(CancellationToken cancellationToken = default)
        {
            // Fast path: already successfully acknowledged — no-op.
            if (Interlocked.CompareExchange(ref _acknowledged, 1, 0) != 0)
                return Task.CompletedTask;

            return AcknowledgeWithResetOnFailureAsync(cancellationToken);
        }

        private async Task AcknowledgeWithResetOnFailureAsync(CancellationToken cancellationToken)
        {
            try
            {
                await _acknowledge(cancellationToken);
            }
            catch
            {
                // Reset the flag so the caller can retry the commit on the same message.
                Interlocked.Exchange(ref _acknowledged, 0);
                throw;
            }
        }
    }
}
