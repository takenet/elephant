using Azure.Messaging.EventHubs.Consumer;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Take.Elephant.Azure
{
    /// <summary>
    /// Implementation of queue on azure event hub.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class AzureEventHubReceiverQueue<T> : IReceiverQueue<T>, IBlockingReceiverQueue<T>, IBatchReceiverQueue<T>, ICloseable, IAsyncDisposable
    {
        private readonly ISerializer<T> _serializer;
        private readonly EventHubConsumerClient _consumer;
        private readonly SemaphoreSlim _consumerStartSemaphore;
        private readonly Channel<string> _channel;
        private readonly CancellationTokenSource _cts;
        private Task _consumerTask;

        public AzureEventHubReceiverQueue(string connectionString, string topic, string consumerGroup, ISerializer<T> serializer)
            : this(new EventHubConsumerClient(consumerGroup, connectionString, topic), serializer)
        {
        }

        public AzureEventHubReceiverQueue(EventHubConsumerClient consumer, ISerializer<T> serializer)
        {
            _consumer = consumer;
            _serializer = serializer;
            _consumerStartSemaphore = new SemaphoreSlim(1, 1);
            _channel = Channel.CreateBounded<string>(1);
            _cts = new CancellationTokenSource();
        }

        public async Task<T> DequeueAsync(CancellationToken cancellationToken)
        {
            await StartConsumerTaskIfNotAsync(cancellationToken).ConfigureAwait(false);
            var value = await _channel.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            return _serializer.Deserialize(value);
        }

        public async Task<IEnumerable<T>> DequeueBatchAsync(int maxBatchSize, CancellationToken cancellationToken)
        {
            await StartConsumerTaskIfNotAsync(cancellationToken).ConfigureAwait(false);
            var list = new List<T>();
            for (int i = 0; i < maxBatchSize; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var value = await _channel.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                list.Add(_serializer.Deserialize(value));
            }
            return list;
        }

        public async Task<T> DequeueOrDefaultAsync(CancellationToken cancellationToken = default)
        {
            await StartConsumerTaskIfNotAsync(cancellationToken).ConfigureAwait(false);
            if (_channel.Reader.TryRead(out var item))
            {
                return _serializer.Deserialize(item);
            }

            return default;
        }

        public async Task CloseAsync(CancellationToken cancellationToken)
        {
            await _consumer.CloseAsync().ConfigureAwait(false);
        }

        public Task OpenAsync(CancellationToken cancellationToken)
        {
            return StartConsumerTaskIfNotAsync(cancellationToken);
        }

        public virtual async ValueTask DisposeAsync()
        {
            _cts.Dispose();
            _consumerStartSemaphore.Dispose();
            await _consumer.DisposeAsync();
        }

        public event EventHandler<ExceptionEventArgs> ConsumerFailed;

        private async Task StartConsumerTaskIfNotAsync(CancellationToken cancellationToken)
        {
            if (_consumerTask != null) return;

            await _consumerStartSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
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
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await foreach (var value in _consumer.ReadEventsAsync(cancellationToken).ConfigureAwait(false))
                    {
                        var resultValue = Encoding.UTF8.GetString(value.Data.Body.Span);
                        await _channel.Writer.WriteAsync(resultValue, cancellationToken).ConfigureAwait(false);
                    }
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
            _channel.Writer.Complete();
        }
    }
}