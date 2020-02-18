using Azure.Messaging.EventHubs.Consumer;
using Dawn;
using System;
using System.Collections.Generic;
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
        private readonly Channel<T> _channel;
        private readonly CancellationTokenSource _cts;
        private Task _consumerTask;

        public AzureEventHubReceiverQueue(string connectionString, string topic, string consumerGroup, ISerializer<T> serializer)
        {
            _consumer = new EventHubConsumerClient(consumerGroup, connectionString, topic);
            _consumerStartSemaphore = new SemaphoreSlim(1, 1);
            _channel = Channel.CreateBounded<T>(1);
            _cts = new CancellationTokenSource();
            _serializer = serializer;
        }

        public async Task<T> DequeueAsync(CancellationToken cancellationToken)
        {
            await StartConsumerTaskIfNotAsync(cancellationToken);
            var value = await _channel.Reader.ReadAsync(cancellationToken);
            return value;
        }

        public async Task<IEnumerable<T>> DequeueBatchAsync(int maxBatchSize, CancellationToken cancellationToken)
        {
            await StartConsumerTaskIfNotAsync(cancellationToken);
            var list = new List<T>();
            for (int i = 0; i < maxBatchSize; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var value = await _channel.Reader.ReadAsync(cancellationToken);
                list.Add(value);
            }
            return list;
        }

        public async Task<T> DequeueOrDefaultAsync(CancellationToken cancellationToken = default)
        {
            await StartConsumerTaskIfNotAsync(cancellationToken);
            if (_channel.Reader.TryRead(out var item))
            {
                return item;
            }

            return default;
        }

        public async Task CloseAsync(CancellationToken cancellationToken)
        {
            await _consumer.CloseAsync();
        }

        public Task OpenAsync(CancellationToken cancellationToken)
        {
            return StartConsumerTaskIfNotAsync(cancellationToken);
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
            await foreach (var value in _consumer.ReadEventsAsync(cancellationToken))
            {
                try
                {
                    var resultValue = _serializer.Deserialize(Encoding.UTF8.GetString(value.Data.Body.Span));
                    await _channel.Writer.WriteAsync(resultValue, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    //todo: event handler
                }
            }
            _channel.Writer.Complete();
        }

        public async ValueTask DisposeAsync()
        {
            await _consumer.DisposeAsync();
        }
    }
}