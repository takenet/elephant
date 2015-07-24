using System.Threading;
using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized
{
    public class FallbackBlockingQueue<T> : Replicator<IBlockingQueue<T>>, IBlockingQueue<T>
    {
        public FallbackBlockingQueue(IBlockingQueue<T> master, IBlockingQueue<T> slave)
            : base(master, slave, new QueueSynchronizer<T>())
        {
        }

        public Task EnqueueAsync(T item)
        {
            return ExecuteWithFallbackAsync(q => q.EnqueueAsync(item));
        }

        public Task<T> DequeueOrDefaultAsync()
        {
            return ExecuteWithFallbackAsync(q => q.DequeueOrDefaultAsync());
        }

        public Task<long> GetLengthAsync()
        {
            return ExecuteWithFallbackAsync(q => q.GetLengthAsync());
        }
        public Task<T> DequeueAsync(CancellationToken cancellationToken)
        {
            return ExecuteWithFallbackAsync(q => q.DequeueAsync(cancellationToken));
        }
    }
}