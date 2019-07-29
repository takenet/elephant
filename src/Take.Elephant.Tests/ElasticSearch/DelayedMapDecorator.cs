using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Tests.Elasticsearch
{
    internal class DelayedMapDecorator<TKey, T> : IMap<TKey, T>
    {
        private readonly IMap<TKey, T> _map;
        private readonly int _delay;

        public DelayedMapDecorator(IMap<TKey,T> map, int delay)
        {
            _map = map;
            _delay = delay;
        }

        public async Task<bool> ContainsKeyAsync(TKey key, CancellationToken cancellationToken = default)
        {
            await Task.Delay(_delay, cancellationToken);
            return await _map.ContainsKeyAsync(key, cancellationToken);
        }

        public async Task<T> GetValueOrDefaultAsync(TKey key, CancellationToken cancellationToken = default)
        {
            await Task.Delay(_delay, cancellationToken);
            return await _map.GetValueOrDefaultAsync(key, cancellationToken);
        }

        public async Task<bool> TryAddAsync(TKey key, T value, bool overwrite = false, CancellationToken cancellationToken = default)
        {
            await Task.Delay(_delay, cancellationToken);
            return await _map.TryAddAsync(key, value, overwrite, cancellationToken);
        }

        public async Task<bool> TryRemoveAsync(TKey key, CancellationToken cancellationToken = default)
        {
            await Task.Delay(_delay, cancellationToken);
            return await _map.TryRemoveAsync(key, cancellationToken);
        }
    }
}
