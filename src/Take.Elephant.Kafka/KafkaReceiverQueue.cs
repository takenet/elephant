using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Take.Elephant.Kafka
{
    public class KafkaReceiverQueue<T> : IReceiverQueue<T>, IBlockingReceiverQueue<T>, IOpenable, ICloseable, IDisposable
    {
        private readonly string _topic;
        private readonly IConsumer<Ignore, string> _consumer;
        private readonly ISerializer<T> _serializer;
        private readonly SemaphoreSlim _consumerStartSemaphore;
        private readonly CancellationTokenSource _cts;
        private readonly Channel<T> _channel;
        private Task _consumerTask;
        private bool _closed;

        public KafkaReceiverQueue(string bootstrapServers, string topic, string groupId, ISerializer<T> serializer, IDeserializer<string> deserializer = null)
            : this(new ConsumerConfig() { BootstrapServers = bootstrapServers, GroupId = groupId }, topic, serializer, deserializer)
        {
        }

        public KafkaReceiverQueue(
            ConsumerConfig consumerConfig,
            string topic,
            ISerializer<T> serializer,
            IDeserializer<string> deserializer = null)
            : this(
                  new ConsumerBuilder<Ignore, string>(consumerConfig)
                      .SetValueDeserializer(deserializer ?? new StringDeserializer())
                      .Build(),
                  serializer,
                  topic)
        {
        }

        public KafkaReceiverQueue(IConsumer<Ignore, string> consumer, ISerializer<T> serializer, string topic)
        {
            if (string.IsNullOrWhiteSpace(topic)) throw new ArgumentException("Value cannot be null or whitespace.", nameof(topic));
            _consumer = consumer ?? throw new ArgumentNullException(nameof(consumer));
            _serializer = serializer;
            _topic = topic;
            _consumerStartSemaphore = new SemaphoreSlim(1, 1);
            _cts = new CancellationTokenSource();
            _channel = Channel.CreateBounded<T>(1);
        }

        public virtual async Task<T> DequeueOrDefaultAsync(CancellationToken cancellationToken = default)
        {
            await StartConsumerTaskIfNotAsync(cancellationToken);
            if (_channel.Reader.TryRead(out var item))
            {
                return item;
            }

            return default;
        }

        public virtual async Task<T> DequeueAsync(CancellationToken cancellationToken)
        {
            await StartConsumerTaskIfNotAsync(cancellationToken);
            return await _channel.Reader.ReadAsync(cancellationToken);
        }

        public Task OpenAsync(CancellationToken cancellationToken)
        {
            return StartConsumerTaskIfNotAsync(cancellationToken);
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

        public event EventHandler<ExceptionEventArgs> ConsumerFailed;

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (!_closed)
                {
                    _consumer.Close();
                }

                _consumer.Dispose();
                _cts.Dispose();
            }
        }

        private async Task StartConsumerTaskIfNotAsync(CancellationToken cancellationToken)
        {
            if (_consumerTask != null) return;

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
            if (_consumer.Subscription.All(s => s != _topic))
            {
                _consumer.Subscribe(_topic);
            }

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var result = _consumer.Consume(cancellationToken);
                    var resultValue = _serializer.Deserialize(result.Value);
                    await _channel.Writer.WriteAsync(resultValue, cancellationToken);
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

            _consumer.Unsubscribe();
            _channel.Writer.Complete();
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

    public class StringDeserializer : IDeserializer<string>
    {
        public string Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            return Deserializers.Utf8.Deserialize(data, isNull, context);
        }
    }
}