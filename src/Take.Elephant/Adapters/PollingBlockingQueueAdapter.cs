using System;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Adapters
{
    /// <summary>
    /// Adapts a <see cref="IQueue{T}"/> to a <see cref="IBlockingQueue{T}"/> using polling with an exponential interval.
    /// </summary>
    public sealed class PollingBlockingQueueAdapter<T> : IBlockingQueue<T>, IDisposable
    {
        private readonly IQueue<T> _queue;
        private readonly int _minDequeueRetryDelay;
        private readonly int _maxDequeueRetryDelay;
        private readonly SemaphoreSlim _dequeueSemaphore;
        
        public PollingBlockingQueueAdapter(
            IQueue<T> queue,
            int minDequeueRetryDelay = 250,
            int maxDequeueRetryDelay = 30000,
            int maxParallelDequeueTasks = 1)
        {
            _queue = queue ?? throw new ArgumentNullException(nameof(queue));
            if (minDequeueRetryDelay <= 0) throw new ArgumentOutOfRangeException(nameof(minDequeueRetryDelay));
            if (maxDequeueRetryDelay <= 0) throw new ArgumentOutOfRangeException(nameof(maxDequeueRetryDelay));
            if (maxDequeueRetryDelay < minDequeueRetryDelay)
            {
                throw new ArgumentException("minDequeueRetryDelay should be smaller than maxDequeueRetryDelay", nameof(maxDequeueRetryDelay));
            }
            if (maxParallelDequeueTasks < 1)
            {
                throw new ArgumentException("maxParallelDequeueTasks should be equal or greater than 1", nameof(maxParallelDequeueTasks));
            }
            _minDequeueRetryDelay = minDequeueRetryDelay;
            _maxDequeueRetryDelay = maxDequeueRetryDelay;
            _dequeueSemaphore = new SemaphoreSlim(1, maxParallelDequeueTasks);
        }

        public Task<T> DequeueOrDefaultAsync(CancellationToken cancellationToken = default) 
            => _queue.DequeueOrDefaultAsync(cancellationToken);

        public Task EnqueueAsync(T item, CancellationToken cancellationToken = default) 
            => _queue.EnqueueAsync(item, cancellationToken);

        public Task<long> GetLengthAsync(CancellationToken cancellationToken = default) 
            => _queue.GetLengthAsync(cancellationToken);

        public async Task<T> DequeueAsync(CancellationToken cancellationToken)
        {
            var item = await DequeueOrDefaultAsync(cancellationToken).ConfigureAwait(false);
            if (item != null) return item;            
            
            // Synchronize the dequeue loop in a semaphore to reduce overhead in concurrent scenarios
            await _dequeueSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

            var tryCount = 0;
            var delay = _minDequeueRetryDelay;            
            
            try
            {
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    item = await DequeueOrDefaultAsync(cancellationToken).ConfigureAwait(false);
                    if (item != null) return item;
                    
                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                    tryCount++;

                    if (delay < _maxDequeueRetryDelay)
                    {
                        delay = _minDequeueRetryDelay * (int) Math.Pow(2, tryCount);
                        if (delay > _maxDequeueRetryDelay)
                        {
                            delay = _maxDequeueRetryDelay;
                        }
                    }
                }
            }
            finally
            {
                _dequeueSemaphore.Release();
            }
        }

        public void Dispose() => _dequeueSemaphore.Dispose();
    }
}