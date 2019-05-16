using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Take.Elephant.Kafka
{
    public class KafkaReceiverQueue<T> : IReceiverQueue<T>, IBlockingReceiverQueue<T>, ICloseable, IDisposable
    {
        private readonly string _topic;
        private readonly IConsumer<Ignore, T> _consumer;
        private readonly Task _consumerTask;
        private readonly SemaphoreSlim _consumerSubscriptionSemaphore;
        private readonly CancellationTokenSource _cts;
        private readonly Channel<T> _channel;
        private bool _subscribed;
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
            _subscribed = _consumer.Subscription.Any(s => s == _topic);
            _cts = new CancellationTokenSource();
            _channel = Channel.CreateBounded<T>(1);
            _consumerTask = Task.Factory.StartNew(
                () => ConsumeAsync(_cts.Token),
                TaskCreationOptions.LongRunning)
                .Unwrap();
        }

        public KafkaReceiverQueue(
            IConsumer<Ignore, T> consumer,
            string topic)
        {
            _consumer = consumer;
            _topic = topic;
            _consumerSubscriptionSemaphore = new SemaphoreSlim(1, 1);
            _subscribed = _consumer.Subscription.Any(s => s == _topic);
            _cts = new CancellationTokenSource();
            _channel = Channel.CreateBounded<T>(1);
            _consumerTask = Task.Factory.StartNew(
                () => ConsumeAsync(_cts.Token),
                TaskCreationOptions.LongRunning)
                .Unwrap();
        }

        private async Task ConsumeAsync(CancellationToken cancellationToken)
        {
            await SubscribeIfNotAsync(cancellationToken);

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var result = _consumer.Consume(cancellationToken);
                    await _channel.Writer.WriteAsync(result.Value, cancellationToken);
                }
                catch (ConsumeException ex) when (!ex.Error.IsError)
                {
                    break;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
            }

            _consumer.Unsubscribe();
            _channel.Writer.Complete();
        }

        private async Task SubscribeIfNotAsync(CancellationToken cancellationToken)
        {
            if (_subscribed) return;
            await _consumerSubscriptionSemaphore.WaitAsync(cancellationToken);
            try
            {
                if (_subscribed) return;
                _consumer.Subscribe(_topic);
                _subscribed = true;
            }
            finally
            {
                _consumerSubscriptionSemaphore.Release();
            }
        }

        public virtual Task<T> DequeueOrDefaultAsync(CancellationToken cancellationToken = default)
        {
            if (_channel.Reader.TryRead(out var item))
            {
                return item.AsCompletedTask();
            }

            return Task.FromResult(default(T));
        }

        public virtual Task<T> DequeueAsync(CancellationToken cancellationToken)
        {
            return _channel.Reader.ReadAsync(cancellationToken).AsTask();
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