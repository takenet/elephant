﻿using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Take.Elephant.Kafka
{
    public class KafkaPartitionQueue<T> : IReceiverQueue<T>, IPartitionSenderQueue<T>, ICloseable, IDisposable
    {
        private readonly KafkaPartitionSenderQueue<T> _senderQueue;
        private readonly KafkaReceiverQueue<T> _receiverQueue;

        public KafkaPartitionQueue(
            ProducerConfig producerConfig,
            ConsumerConfig consumerConfig,
            string topic,
            Take.Elephant.ISerializer<T> serializer,
            Confluent.Kafka.ISerializer<string> kafkaSerializer = null,
            IDeserializer<string> kafkaDeserializer = null)
        {
            _senderQueue = new KafkaPartitionSenderQueue<T>(producerConfig, topic, serializer, kafkaSerializer);
            _receiverQueue = new KafkaReceiverQueue<T>(consumerConfig, topic, serializer, kafkaDeserializer);
        }

        public Task EnqueueAsync(T item, string key, CancellationToken cancellationToken = default)
        {
            return _senderQueue.EnqueueAsync(item, key, cancellationToken);
        }

        public Task<T> DequeueOrDefaultAsync(CancellationToken cancellationToken = default)
        {
            return _receiverQueue.DequeueOrDefaultAsync(cancellationToken);
        }

        public Task<T> DequeueAsync(CancellationToken cancellationToken = default)
        {
            return _receiverQueue.DequeueAsync(cancellationToken);
        }

        public Task CloseAsync(CancellationToken cancellationToken) => _receiverQueue.CloseAsync(cancellationToken);

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected void Dispose(bool disposing)
        {
            if (disposing)
            {
                _senderQueue?.Dispose();
                _receiverQueue?.Dispose();
            }
        }
    }
}
