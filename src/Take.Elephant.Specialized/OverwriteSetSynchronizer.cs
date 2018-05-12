using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Specialized
{
    public class OverwriteSetSynchronizer<T> : ISynchronizer<ISet<T>>
    {
        private readonly TimeSpan _synchronizationTimeout;

        public OverwriteSetSynchronizer(TimeSpan synchronizationTimeout)
        {
            // Only to validate the parameter
            new CancellationTokenSource(_synchronizationTimeout).Dispose();
            _synchronizationTimeout = synchronizationTimeout;
        }

        public virtual async Task SynchronizeAsync(ISet<T> source, ISet<T> target)
        {
            using (var cts = new CancellationTokenSource(_synchronizationTimeout))
            {
                var targetEnumerable = await target.AsEnumerableAsync().ConfigureAwait(false);
                // Copy the keys to be removed to the memory, to avoid problems with changing the collection while enumerating.
                var targetToRemove = new List<T>();
                await targetEnumerable.ForEachAsync(async v =>
                {
                    if (!await source.ContainsAsync(v).ConfigureAwait(false)) targetToRemove.Add(v);
                }, cts.Token);

                foreach (var key in targetToRemove)
                {
                    await target.TryRemoveAsync(key).ConfigureAwait(false);
                }

                var sourceEnumerable = await source.AsEnumerableAsync().ConfigureAwait(false);
                await sourceEnumerable.ForEachAsync(async v =>
                {                    
                    await target.AddAsync(v).ConfigureAwait(false);
                }, cts.Token);
            }
        }
    }
}
