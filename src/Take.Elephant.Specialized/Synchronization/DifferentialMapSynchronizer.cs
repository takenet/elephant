using System;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Specialized.Synchronization
{
    public sealed class DifferentialMapSynchronizer<TKey, TValue> : ISynchronizer<IMap<TKey, TValue>>
    {
        private readonly TimeSpan _synchronizationTimeout;

        public DifferentialMapSynchronizer(TimeSpan synchronizationTimeout)
        {
            // Only to validate the parameter
            new CancellationTokenSource(_synchronizationTimeout).Dispose();            
            _synchronizationTimeout = synchronizationTimeout;
        }

        public async Task SynchronizeAsync(IMap<TKey, TValue> source, IMap<TKey, TValue> target)
        {
            // TODO: The slave deleted values are not being handled
            var sourceKeysMap = source as IKeysMap<TKey, TValue>;
            if (sourceKeysMap == null) throw new ArgumentException("The source map must implement IKeysMap to allow synchronization");
            
            using (var cts = new CancellationTokenSource(_synchronizationTimeout))
            {
                var sourceKeysEnumerable = await sourceKeysMap.GetKeysAsync().ConfigureAwait(false);
                await sourceKeysEnumerable.ForEachAsync(async key =>
                {
                    var value = await source.GetValueOrDefaultAsync(key).ConfigureAwait(false);
                    await target.TryAddAsync(key, value, false).ConfigureAwait(false);
                }, cts.Token);
            }
        }
    }
}
