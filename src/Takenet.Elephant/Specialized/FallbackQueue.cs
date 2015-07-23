using System;
using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized
{
    public class FallbackQueue<T> : Replicator<IQueue<T>>, IQueue<T>
    {        
        public FallbackQueue(IQueue<T> master, IQueue<T> slave)
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
    }
}
