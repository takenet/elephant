using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized
{
    internal class MapSynchronizer<TKey, TValue> : ISynchronizer<IMap<TKey, TValue>>
    {
        private readonly TimeSpan _synchronizationTimeout;

        public MapSynchronizer(TimeSpan synchronizationTimeout)
        {
            _synchronizationTimeout = synchronizationTimeout;
        }

        public async Task SynchronizeAsync(IMap<TKey, TValue> master, IMap<TKey, TValue> slave)
        {
            var slaveKeysMap = slave as IKeysMap<TKey, TValue>;
            if (slaveKeysMap == null) throw new ArgumentException("The slave map must implement IKeysMap to allow synchronization");

            var slaveKeysEnumerable = await slaveKeysMap.GetKeysAsync().ConfigureAwait(false);
            await slaveKeysEnumerable.ForEachAsync(async key =>
            {
                var value = await slave.GetValueOrDefaultAsync(key).ConfigureAwait(false);
                await master.TryAddAsync(key, value, false).ConfigureAwait(false);
            }, new CancellationTokenSource(_synchronizationTimeout).Token);            
        }
    }
}
