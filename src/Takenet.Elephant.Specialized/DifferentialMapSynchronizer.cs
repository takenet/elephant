using System;
using System.Threading;
using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized
{
    internal class DifferentialMapSynchronizer<TKey, TValue> : ISynchronizer<IMap<TKey, TValue>>
    {
        private readonly TimeSpan _synchronizationTimeout;

        public DifferentialMapSynchronizer(TimeSpan synchronizationTimeout)
        {
            // Only to validate the parameter
            new CancellationTokenSource(_synchronizationTimeout).Dispose();            
            _synchronizationTimeout = synchronizationTimeout;
        }

        public async Task SynchronizeAsync(IMap<TKey, TValue> first, IMap<TKey, TValue> second)
        {
            // TODO: The slave deleted values are not being handled
            var slaveKeysMap = second as IKeysMap<TKey, TValue>;
            if (slaveKeysMap == null) throw new ArgumentException("The slave map must implement IKeysMap to allow synchronization");
            
            using (var cts = new CancellationTokenSource(_synchronizationTimeout))
            {
                var slaveKeysEnumerable = await slaveKeysMap.GetKeysAsync().ConfigureAwait(false);
                await slaveKeysEnumerable.ForEachAsync(async key =>
                {
                    var value = await second.GetValueOrDefaultAsync(key).ConfigureAwait(false);
                    await first.TryAddAsync(key, value, false).ConfigureAwait(false);
                }, cts.Token);
            }
        }
    }
}
