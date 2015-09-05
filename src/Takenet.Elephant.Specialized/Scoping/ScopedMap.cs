using System;
using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized.Scoping
{
    public class ScopedMap<TKey, TValue> : IMap<TKey, TValue>
    {
        private readonly IMap<TKey, TValue> _map;
        private readonly MapScope _scope;
        private readonly ISerializer<TKey> _keySerializer;

        public ScopedMap(IMap<TKey, TValue> map, IScope scope, ISerializer<TKey> keySerializer)
        {
            if (scope == null) throw new ArgumentNullException(nameof(scope));
            if (!(scope is MapScope)) throw new ArgumentException("The provided scope type is incompatible", nameof(scope));
            if (map == null) throw new ArgumentNullException(nameof(map));
            if (keySerializer == null) throw new ArgumentNullException(nameof(keySerializer));
            _map = map;
            _scope = (MapScope)scope;
            _scope.RemoveKeyFunc = k => _map.TryRemoveAsync(_keySerializer.Deserialize(k));            
            _keySerializer = keySerializer;
        }

        public async Task<bool> TryAddAsync(TKey key, TValue value, bool overwrite = false)
        {
            if (!await _map.TryAddAsync(key, value, overwrite).ConfigureAwait(false)) return false;
            await _scope.AddKeyAsync(_keySerializer.Serialize(key)).ConfigureAwait(false);
            return true;
        }

        public Task<TValue> GetValueOrDefaultAsync(TKey key)
        {
            return _map.GetValueOrDefaultAsync(key);
        }

        public async Task<bool> TryRemoveAsync(TKey key)
        {
            if (!await _map.TryRemoveAsync(key).ConfigureAwait(false)) return false;
            await _scope.RemoveKeyAsync(_keySerializer.Serialize(key)).ConfigureAwait(false);
            return true;
        }

        public Task<bool> ContainsKeyAsync(TKey key)
        {
            return _map.ContainsKeyAsync(key);
        }
    }
}