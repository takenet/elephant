using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Specialized
{
    public sealed class CopyMapSynchronizer<TKey, TValue> : ISynchronizer<IMap<TKey, TValue>>
    {
        private readonly TimeSpan _synchronizationTimeout;

        public CopyMapSynchronizer(TimeSpan synchronizationTimeout)
        {
            // Only to validate the parameter
            new CancellationTokenSource(_synchronizationTimeout).Dispose();
            _synchronizationTimeout = synchronizationTimeout;
        }

        public async Task SynchronizeAsync(IMap<TKey, TValue> source, IMap<TKey, TValue> target)
        {
            var sourceKeysMap = source as IKeysMap<TKey, TValue>;
            if (sourceKeysMap == null) throw new ArgumentException("The source map must implement IKeysMap to allow synchronization");

            var targetKeysMap = target as IKeysMap<TKey, TValue>;
            if (targetKeysMap == null) throw new ArgumentException("The target map must implement IKeysMap to allow synchronization");

            using (var cts = new CancellationTokenSource(_synchronizationTimeout))
            {
                // Clean the target
                var targetKeysEnumerable = await targetKeysMap.GetKeysAsync().ConfigureAwait(false);
                var keysToRemove = new List<TKey>();
                await targetKeysEnumerable.ForEachAsync(key => keysToRemove.Add(key), cts.Token);
                foreach (var key in keysToRemove)
                {
                    await target.TryRemoveAsync(key);
                }

                // Copy the source to the target
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
