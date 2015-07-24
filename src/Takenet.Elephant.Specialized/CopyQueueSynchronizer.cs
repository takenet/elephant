using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized
{
    public sealed class CopyQueueSynchronizer<T> : ISynchronizer<IQueue<T>>
    {
        public async Task SynchronizeAsync(IQueue<T> source, IQueue<T> target)
        {
            while (await source.GetLengthAsync().ConfigureAwait(false) > 0)
            {
                var value = await source.DequeueOrDefaultAsync().ConfigureAwait(false);
                if (value != null && !value.Equals(default(T))) await target.EnqueueAsync(value).ConfigureAwait(false);
            }            
        }
    }
}