using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized
{
    public class OverwriteMapSynchronizer<TKey, TValue> : ISynchronizer<IMap<TKey, TValue>>
    {
        private readonly TimeSpan _synchronizationTimeout;

        public OverwriteMapSynchronizer(TimeSpan synchronizationTimeout)
        {
            // Only to validate the parameter
            new CancellationTokenSource(_synchronizationTimeout).Dispose();
            _synchronizationTimeout = synchronizationTimeout;
        }

        public virtual async Task SynchronizeAsync(IMap<TKey, TValue> source, IMap<TKey, TValue> target)
        {
            var sourceKeysMap = source as IKeysMap<TKey, TValue>;
            if (sourceKeysMap == null) throw new ArgumentException("The source map must implement IKeysMap to allow synchronization");

            var targetKeysMap = target as IKeysMap<TKey, TValue>;
            if (targetKeysMap == null) throw new ArgumentException("The target map must implement IKeysMap to allow synchronization");


            using (var cts = new CancellationTokenSource(_synchronizationTimeout))
            {
                var targetKeysEnumerable = await targetKeysMap.GetKeysAsync().ConfigureAwait(false);
                // Copy the keys to be removed to the memory, to avoid problems with changing the collection while enumerating.
                var targetKeysToRemove = new List<TKey>();
                await targetKeysEnumerable.ForEachAsync(async key =>
                {
                    if (!await source.ContainsKeyAsync(key).ConfigureAwait(false)) targetKeysToRemove.Add(key);
                }, cts.Token);

                foreach (var key in targetKeysToRemove)
                {
                    await target.TryRemoveAsync(key).ConfigureAwait(false);
                }

                var sourceKeysEnumerable = await sourceKeysMap.GetKeysAsync().ConfigureAwait(false);
                await sourceKeysEnumerable.ForEachAsync(async key =>
                {
                    var value = await source.GetValueOrDefaultAsync(key).ConfigureAwait(false);
                    await target.TryAddAsync(key, value, true).ConfigureAwait(false);
                }, cts.Token);
            }
        }
    }
}
