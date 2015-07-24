using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized
{
    internal sealed class CopyQueueSynchronizer<T> : ISynchronizer<IQueue<T>>
    {
        public async Task SynchronizeAsync(IQueue<T> first, IQueue<T> second)
        {
            while (await second.GetLengthAsync().ConfigureAwait(false) > 0)
            {
                var value = await second.DequeueOrDefaultAsync().ConfigureAwait(false);
                if (value != null && !value.Equals(default(T))) await first.EnqueueAsync(value).ConfigureAwait(false);
            }            
        }
    }
}