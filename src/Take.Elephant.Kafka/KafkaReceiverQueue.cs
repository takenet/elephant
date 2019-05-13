using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Kafka
{
    public class KafkaReceiverQueue<T> : IReceiverQueue<T>, IBlockingReceiverQueue<T>, ICloseable, IDisposable
    {
        private readonly string _topic;
        private readonly IConsumer<Ignore, T> _consumer;
        private readonly Task _consumerTask;
        private readonly SemaphoreSlim _consumerSubscriptionSemaphore;
        private bool _closed;

        public KafkaReceiverQueue(string bootstrapServers, string topic, string groupId, IDeserializer<T> deserializer)
            : this(new ConsumerConfig() { BootstrapServers = bootstrapServers, GroupId = groupId }, topic, deserializer)
        {
        }

        public KafkaReceiverQueue(
            ConsumerConfig consumerConfig,
            string topic,
            IDeserializer<T> deserializer)
        {
            _consumer = new ConsumerBuilder<Ignore, T>(consumerConfig)
                .SetValueDeserializer(deserializer)
                .Build();

            _topic = topic;
            _consumerSubscriptionSemaphore = new SemaphoreSlim(1, 1);
        }

        private async Task SubscribeIfNotAsync(CancellationToken cancellationToken)
        {
            if (_consumer.Subscription.Any(s => s == _topic))
            {
                return;
            }

            await _consumerSubscriptionSemaphore.WaitAsync(cancellationToken);
            try
            {
                if (_consumer.Subscription.Any(s => s == _topic))
                {
                    return;
                }

                _consumer.Subscribe(_topic);
            }
            finally
            {
                _consumerSubscriptionSemaphore.Release();
            }
        }

        public virtual async Task<T> DequeueOrDefaultAsync(CancellationToken cancellationToken = default)
        {
            await SubscribeIfNotAsync(cancellationToken);

            using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5)))
            using (var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, cancellationToken))
            {
                try
                {
                    var consumeResult = _consumer.Consume(linkedCts.Token);
                    return consumeResult.Value;
                }
                catch (OperationCanceledException) when (cts.IsCancellationRequested)
                {
                    return default;
                }
            }
        }

        public virtual async Task<T> DequeueAsync(CancellationToken cancellationToken)
        {
            await SubscribeIfNotAsync(cancellationToken);
            return _consumer.Consume(cancellationToken).Value;
        }

        public virtual Task CloseAsync(CancellationToken cancellationToken)
        {
            if (!_closed)
            {
                _consumer.Close();
                _closed = true;
            }

            return _consumerTask;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (!_closed)
                {
                    _consumer.Close();
                }

                _consumer.Dispose();
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }

    public class JsonDeserializer<T> : IDeserializer<T>
    {
        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            var json = Deserializers.Utf8.Deserialize(data, isNull, context);
            return JsonConvert.DeserializeObject<T>(json);
        }
    }
}