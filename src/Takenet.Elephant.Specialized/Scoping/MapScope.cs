using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized.Scoping
{
    public class MapScope : IScope
    {
        private readonly ISetMap<string, IdentifierKey> _keysSetMap;
        private readonly IDictionary<string, IScopedMap> _scopedMapDictionary;

        public MapScope(string name, ISetMap<string, IdentifierKey> keysSetMap)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));
            if (keysSetMap == null) throw new ArgumentNullException(nameof(keysSetMap));
            Name = name;
            _keysSetMap = keysSetMap;
            _scopedMapDictionary = new Dictionary<string, IScopedMap>();
        }

        internal void Register(IScopedMap scopedMap)
        {
            _scopedMapDictionary.Add(scopedMap.Identifier, scopedMap);
        }

        internal void Unregister(string identifier)
        {
            _scopedMapDictionary.Remove(identifier);
        }

        public string Name { get; }

        public virtual async Task ClearAsync(CancellationToken cancellationToken)
        {
            var keysSet = await _keysSetMap.GetValueOrDefaultAsync(Name).ConfigureAwait(false);
            if (keysSet != null)
            {
                var keysEnumerable = await keysSet.AsEnumerableAsync().ConfigureAwait(false);
                await keysEnumerable.ForEachAsync(async k =>
                {
                    IScopedMap scopedMap;
                    if (_scopedMapDictionary.TryGetValue(k.Identifier, out scopedMap))
                    {
                        await scopedMap.RemoveKeyAsync(k.Key).ConfigureAwait(false);
                    }

                }, 
                cancellationToken);
            }

            await _keysSetMap.TryRemoveAsync(Name).ConfigureAwait(false);            
        }

        protected internal virtual Task AddKeyAsync(string identifier, string key)
        {
            if (!_scopedMapDictionary.ContainsKey(identifier)) throw new InvalidOperationException("The scoped map must be registered before changing the scope");
            return _keysSetMap.AddItemAsync(Name, new IdentifierKey() { Identifier = identifier, Key =  key});
        }

        protected internal virtual Task RemoveKeyAsync(string identifier, string key)
        {
            if (!_scopedMapDictionary.ContainsKey(identifier)) throw new InvalidOperationException("The scoped map must be registered before changing the scope");
            return _keysSetMap.TryRemoveItemAsync(Name, new IdentifierKey() { Identifier = identifier, Key = key });
        }
    }
}