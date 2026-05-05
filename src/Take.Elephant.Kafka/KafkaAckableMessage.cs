using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Kafka
{
    /// <summary>
    /// A Kafka message envelope with payload, headers, and an <see cref="AcknowledgeAsync"/> delegate
    /// that commits the offset to the broker. Call only after processing is fully complete.
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

        public T Item { get; }

        public IReadOnlyDictionary<string, byte[]> Headers { get; }

        public string Topic { get; }

        public int Partition { get; }

        public long Offset { get; }

        public bool IsAcknowledged => _acknowledged == 1;

        /// <summary>
        /// Commits the offset to the broker. Idempotent on success; if the commit throws,
        /// <see cref="IsAcknowledged"/> remains <see langword="false"/> allowing retry.
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
