using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Take.Elephant.Adapters
{
    /// <summary>
    /// Adapts a <see cref="IQueue{T}"/> to a <see cref="IBlockingQueue{T}"/> using a bus to publish when new items are enqueued,
    /// reducing the polling overhead.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public sealed class BusQueueBlockingQueueAdapter<T> : IBlockingQueue<T>, IDisposable
    {
        private readonly IQueue<T> _queue;
        private readonly IBus<string, string> _bus;
        private readonly string _busChannelName;
        private readonly string _publishMessage;
        private readonly Channel<T> _channel;

        private readonly SemaphoreSlim _subscriptionSemaphore;
        private bool _subscribed;
        private int _dequeuingCount;

        public BusQueueBlockingQueueAdapter(
            IQueue<T> queue,
            IBus<string, string> bus,
            string busChannelName,
            string publishMessage = "new-item")
        {
            _queue = queue ?? throw new ArgumentNullException(nameof(queue));
            _bus = bus ?? throw new ArgumentNullException(nameof(bus));
            _busChannelName = busChannelName ?? throw new ArgumentNullException(nameof(busChannelName));
            _publishMessage = publishMessage;
            _channel = Channel.CreateUnbounded<T>();
            
            _subscriptionSemaphore = new SemaphoreSlim(1);
        }
        
        public async Task EnqueueAsync(T item, CancellationToken cancellationToken = default)
        {
            await _queue.EnqueueAsync(item, cancellationToken);
            await _bus.PublishAsync(_busChannelName, _publishMessage, cancellationToken);
        }

        public Task<long> GetLengthAsync(CancellationToken cancellationToken = default)
        {
            return _queue.GetLengthAsync(cancellationToken);
        }

        public Task<T> DequeueOrDefaultAsync(CancellationToken cancellationToken = default)
        {
            return _queue.DequeueOrDefaultAsync(cancellationToken);
        }
        
        public async Task<T> DequeueAsync(CancellationToken cancellationToken)
        {
            var item = await DequeueOrDefaultAsync(cancellationToken).ConfigureAwait(false);
            if (item != null) return item;
            
            if (!_subscribed)
            {
                await _subscriptionSemaphore.WaitAsync(cancellationToken);
                try
                {
                    await _bus.SubscribeAsync(_busChannelName, HandlePublishedItemAsync, cancellationToken).ConfigureAwait(false);
                    _subscribed = true;
                }
                finally
                {
                    _subscriptionSemaphore.Release();
                }
            }
            
            Interlocked.Increment(ref _dequeuingCount);
            try
            {
                return await _channel.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                Interlocked.Decrement(ref _dequeuingCount);
            }
        }

        public void Dispose()
        {
            _subscriptionSemaphore.Dispose();
        }
        
        private async Task HandlePublishedItemAsync(string channel, string message, CancellationToken cancellationToken)
        {
            try
            {
                // To reduce the overhead, just check if there's any pending dequeue task
                if (_dequeuingCount > 0)
                {
                    var item = await DequeueOrDefaultAsync(cancellationToken).ConfigureAwait(false);
                    if (item != null)
                    {
                        await _channel.Writer.WriteAsync(item, cancellationToken).ConfigureAwait(false);
                    }
                }
            }
            catch (Exception ex)
            {
                Trace.TraceError(ex.ToString());
            }
        }
    }
}