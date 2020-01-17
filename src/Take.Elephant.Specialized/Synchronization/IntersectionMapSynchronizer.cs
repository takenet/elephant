using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Specialized.Synchronization
{
    public sealed class IntersectionMapSynchronizer<TKey, TValue> : ISynchronizer<IMap<TKey, TValue>>
    {
        private readonly TimeSpan _synchronizationTimeout;

        public IntersectionMapSynchronizer(TimeSpan synchronizationTimeout)
        {
            // Only to validate the parameter
            new CancellationTokenSource(_synchronizationTimeout).Dispose();
            _synchronizationTimeout = synchronizationTimeout;
        }

        public async Task SynchronizeAsync(IMap<TKey, TValue> source, IMap<TKey, TValue> target)
        {
            var targetKeysMap = target as IKeysMap<TKey, TValue>;
            if (targetKeysMap == null) throw new ArgumentException("The target map must implement IKeysMap to allow synchronization");

            using (var cts = new CancellationTokenSource(_synchronizationTimeout))
            {
                var targetKeysEnumerable = await targetKeysMap.GetKeysAsync().ConfigureAwait(false);
                var keysToRemove = new List<TKey>();
                await targetKeysEnumerable.ForEachAsync(async key =>
                {
                    if (await source.ContainsKeyAsync(key))
                    {
                        var value = await source.GetValueOrDefaultAsync(key).ConfigureAwait(false);
                        await target.TryAddAsync(key, value, true).ConfigureAwait(false);
                    }
                    else
                    {
                        keysToRemove.Add(key);                        
                    }
                }, cts.Token);

                foreach (var key in keysToRemove)
                {
                    await target.TryRemoveAsync(key);
                }
            }
        }
    }
}
