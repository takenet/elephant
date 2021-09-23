using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Take.Elephant.Kafka
{
    public class PartitionedKafkaReceiver<TKey, TEvent> : IEventStreamConsumer<TKey, TEvent>, IOpenable, ICloseable, IDisposable
    {
        private readonly IConsumer<TKey, TEvent> _partitionedConsumer;
        private readonly SemaphoreSlim _consumerStartSemaphore;
        private readonly CancellationTokenSource _cts;
        private readonly Channel<Message<TKey, TEvent>> _channel;
        private Task _consumerTask;
        private bool _closed;

        public PartitionedKafkaReceiver(string bootstrapServers, string topic, string groupId, ISerializer<TEvent> serializer, IDeserializer<TEvent> deserializer = null)
            : this(new ConsumerConfig() { BootstrapServers = bootstrapServers, GroupId = groupId }, topic, serializer, deserializer)
        {
        }

        public PartitionedKafkaReceiver(
            ConsumerConfig consumerConfig,
            string topic,
            ISerializer<TEvent> serializer,
            IDeserializer<TEvent> deserializer = null)
            : this(
                  new ConsumerBuilder<TKey, TEvent>(consumerConfig)
                      .SetValueDeserializer(deserializer ?? new Deserializer<TEvent>(serializer))
                      .Build(),
                    serializer,
                  topic)
        {
        }

        public PartitionedKafkaReceiver(IConsumer<TKey, TEvent> consumer, ISerializer<TEvent> serializer, string topic)
        {
            if (string.IsNullOrWhiteSpace(topic))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(topic));
            _partitionedConsumer = consumer ?? throw new ArgumentNullException(nameof(consumer));
            Topic = topic;
            _consumerStartSemaphore = new SemaphoreSlim(1, 1);
            _cts = new CancellationTokenSource();
            _channel = Channel.CreateBounded<Message<TKey, TEvent>>(1);
        }

        public string Topic { get; }

        public virtual async Task<(TKey key, TEvent item)> ConsumeOrDefaultAsync(CancellationToken cancellationToken)
        {
            await StartConsumerTaskIfNotAsync(cancellationToken);
            if (_channel.Reader.TryRead(out var message))
            {
                return (message.Key, message.Value);
            }

            return default;
        }

        public Task OpenAsync(CancellationToken cancellationToken)
        {
            return StartConsumerTaskIfNotAsync(cancellationToken);
        }

        public virtual Task CloseAsync(CancellationToken cancellationToken)
        {
            if (!_closed)
            {
                _partitionedConsumer.Close();
                _closed = true;
            }

            return _consumerTask;
        }

        public event EventHandler<ExceptionEventArgs> ConsumerFailed;

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (!_closed)
                {
                    _partitionedConsumer.Close();
                }

                _partitionedConsumer.Dispose();
                _cts.Dispose();
            }
        }

        private async Task StartConsumerTaskIfNotAsync(CancellationToken cancellationToken)
        {
            if (_consumerTask != null)
                return;

            await _consumerStartSemaphore.WaitAsync(cancellationToken);
            try
            {
                if (_consumerTask == null)
                {
                    _consumerTask = Task
                        .Factory
                        .StartNew(
                            () => ConsumeAsync(_cts.Token),
                            TaskCreationOptions.LongRunning)
                        .Unwrap();
                }
            }
            finally
            {
                _consumerStartSemaphore.Release();
            }
        }

        private async Task ConsumeAsync(CancellationToken cancellationToken)
        {
            if (_partitionedConsumer.Subscription.All(s => s != Topic))
            {
                _partitionedConsumer.Subscribe(Topic);
            }

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var result = _partitionedConsumer.Consume(cancellationToken);
                    await _channel.Writer.WriteAsync(result.Message, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    var handler = ConsumerFailed;
                    if (handler != null)
                    {
                        handler.Invoke(this, new ExceptionEventArgs(ex));
                    }
                    else
                    {
                        Trace.TraceError("An unhandled exception occurred on KafkaReceiverQueue: {0}", ex);
                    }
                }
            }

            _partitionedConsumer.Unsubscribe();
            _channel.Writer.Complete();
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public class Deserializer<T> : IDeserializer<T>
        {
            private readonly ISerializer<T> _serializer;

            public Deserializer(ISerializer<T> serializer)
            {
                _serializer = serializer;
            }
            public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
            {
                return _serializer.Deserialize(Deserializers.Utf8.Deserialize(data, isNull, context));
            }
        }
    }
}