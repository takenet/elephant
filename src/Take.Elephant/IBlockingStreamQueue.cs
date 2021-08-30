using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant
{
    public interface IBlockingStreamQueue<T> : IBlockingQueue<T>
    {
        Task EnqueueAsync(T item, string id, CancellationToken cancellationToken = default(CancellationToken));
    }
}
