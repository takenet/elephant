using System.Threading;
using System.Threading.Tasks;

namespace Takenet.Elephant.Memory
{
    /// <summary>
    /// Implements the <see cref="IQueueMap{TKey,TItem}"/> interface using the <see cref="Map{TKey,TItem}"/> and <see cref="Queue{T}"/> classes.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    public class QueueMap<TKey, TItem> : Map<TKey, IBlockingQueue<TItem>>, IBlockingQueueMap<TKey, TItem>, IQueueMap<TKey, TItem>
    {
        public QueueMap()
            : base(() => new Queue<TItem>())
        {

        }

        public virtual Task<bool> TryAddAsync(TKey key, IQueue<TItem> value, bool overwrite = false) => 
            base.TryAddAsync(key, new BlockingQueueWrapper<TItem>(value), overwrite);

        async Task<IQueue<TItem>> IMap<TKey, IQueue<TItem>>.GetValueOrDefaultAsync(TKey key) => 
            await GetValueOrDefaultAsync(key).ConfigureAwait(false);

        private class BlockingQueueWrapper<T> : IBlockingQueue<T>
        {
            private readonly IQueue<T> _queue;

            public BlockingQueueWrapper(IQueue<T> queue)
            {
                _queue = queue;
            }

            public virtual Task EnqueueAsync(T item)
            {
                return _queue.EnqueueAsync(item);
            }

            public virtual Task<T> DequeueOrDefaultAsync()
            {
                return _queue.DequeueOrDefaultAsync();
            }

            public virtual Task<long> GetLengthAsync()
            {
                return _queue.GetLengthAsync();
            }

            public virtual Task<T> DequeueAsync(CancellationToken cancellationToken)
            {
                throw new System.NotSupportedException();
            }
        }
    }
}