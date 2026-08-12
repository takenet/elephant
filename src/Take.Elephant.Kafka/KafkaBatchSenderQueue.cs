using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Take.Elephant.Kafka
{
    /// <summary>
    /// A Kafka sender queue that publishes each item as an individual message,
    /// relying on the Kafka producer's built-in batching (LingerMs / BatchSize) to
    /// aggregate them into fewer network requests instead of serializing everything
    /// into a single oversized payload.
    /// </summary>
    public class KafkaBatchSenderQueue<T> : KafkaSenderQueue<T>, IBatchSenderQueue<T>
    {
        public KafkaBatchSenderQueue(
            string bootstrapServers,
            string topic,
            ISerializer<T> serializer
        )
            : base(bootstrapServers, topic, serializer) { }

        public KafkaBatchSenderQueue(
            string bootstrapServers,
            string topic,
            ISerializer<T> serializer,
            Confluent.Kafka.ISerializer<string> kafkaSerializer
        )
            : base(bootstrapServers, topic, serializer, kafkaSerializer) { }

        public KafkaBatchSenderQueue(
            ProducerConfig producerConfig,
            string topic,
            ISerializer<T> serializer
        )
            : base(producerConfig, topic, serializer) { }

        public KafkaBatchSenderQueue(
            ProducerConfig producerConfig,
            string topic,
            ISerializer<T> serializer,
            Confluent.Kafka.ISerializer<string> kafkaSerializer
        )
            : base(producerConfig, topic, serializer, kafkaSerializer) { }

        public KafkaBatchSenderQueue(
            ProducerConfig producerConfig,
            string topic,
            ISerializer<T> serializer,
            Confluent.Kafka.ISerializer<string> kafkaSerializer,
            IKafkaHeaderProvider headerProvider
        )
            : base(producerConfig, topic, serializer, kafkaSerializer, headerProvider) { }

        public KafkaBatchSenderQueue(
            IProducer<Null, string> producer,
            ISerializer<T> serializer,
            string topic
        )
            : base(producer, serializer, topic) { }

        public KafkaBatchSenderQueue(
            IProducer<Null, string> producer,
            ISerializer<T> serializer,
            string topic,
            IKafkaHeaderProvider headerProvider
        )
            : base(producer, serializer, topic, headerProvider) { }

        public KafkaBatchSenderQueue(
            IEventStreamPublisher<Null, string> producer,
            ISerializer<T> serializer,
            string topic
        )
            : base(producer, serializer, topic) { }

        /// <inheritdoc/>
        public Task EnqueueBatchAsync(
            IEnumerable<T> items,
            CancellationToken cancellationToken = default
        )
        {
            ArgumentNullException.ThrowIfNull(items);
            // All EnqueueAsync calls are started before any awaiting, so the Kafka
            // producer can coalesce them into a single network request via LingerMs/BatchSize.
            return Task.WhenAll(items.Select(item => EnqueueAsync(item, cancellationToken)));
        }
    }
}
