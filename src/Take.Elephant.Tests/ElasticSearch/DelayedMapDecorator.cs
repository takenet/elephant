using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Tests.ElasticSearch
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

        public Task<bool> ContainsKeyAsync(TKey key, CancellationToken cancellationToken = default)
        {
            Thread.Sleep(_delay);
            return _map.ContainsKeyAsync(key, cancellationToken);
        }

        public Task<T> GetValueOrDefaultAsync(TKey key, CancellationToken cancellationToken = default)
        {
            Thread.Sleep(_delay);
            return _map.GetValueOrDefaultAsync(key, cancellationToken);
        }

        public Task<bool> TryAddAsync(TKey key, T value, bool overwrite = false, CancellationToken cancellationToken = default)
        {
            Thread.Sleep(_delay);
            return _map.TryAddAsync(key, value, overwrite, cancellationToken);
        }

        public Task<bool> TryRemoveAsync(TKey key, CancellationToken cancellationToken = default)
        {
            Thread.Sleep(_delay);
            return _map.TryRemoveAsync(key, cancellationToken);
        }
    }
}
