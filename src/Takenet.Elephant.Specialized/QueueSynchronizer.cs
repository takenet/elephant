using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized
{
    internal sealed class QueueSynchronizer<T> : ISynchronizer<IQueue<T>>
    {
        public async Task SynchronizeAsync(IQueue<T> master, IQueue<T> slave)
        {
            while (await slave.GetLengthAsync().ConfigureAwait(false) > 0)
            {
                var value = await slave.DequeueOrDefaultAsync().ConfigureAwait(false);
                if (value != null && !value.Equals(default(T))) await master.EnqueueAsync(value).ConfigureAwait(false);
            }            
        }
    }
}
