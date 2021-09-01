using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Memory
{
    public class StreamQueue<T> : Queue<T>, IStreamSenderQueue<T>
    {
        public StreamQueue() : base() { }

        public virtual Task EnqueueAsync(T item, string id, CancellationToken cancellationToken = default(CancellationToken))
        {
           return EnqueueAsync(item, cancellationToken);
        }
    }
}

