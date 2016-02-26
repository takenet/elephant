using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized.Scoping
{
    public class MapScope : IScope
    {
        protected readonly ISetMap<string, IdentifierKey> KeysSetMap;
        protected readonly IDictionary<string, IScopedMap> ScopedMapDictionary;

        public MapScope(string name, ISetMap<string, IdentifierKey> keysSetMap)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));
            if (keysSetMap == null) throw new ArgumentNullException(nameof(keysSetMap));
            Name = name;
            KeysSetMap = keysSetMap;
            ScopedMapDictionary = new Dictionary<string, IScopedMap>();
        }

        internal void Register(IScopedMap scopedMap)
        {
            ScopedMapDictionary.Add(scopedMap.Identifier, scopedMap);
        }

        internal void Unregister(string identifier)
        {
            ScopedMapDictionary.Remove(identifier);
        }

        public string Name { get; }

        public virtual async Task ClearAsync(CancellationToken cancellationToken)
        {
            var keysSet = await KeysSetMap.GetValueOrDefaultAsync(Name).ConfigureAwait(false);
            if (keysSet != null)
            {
                var keysEnumerable = await keysSet.AsEnumerableAsync().ConfigureAwait(false);
                await keysEnumerable.ForEachAsync(async k =>
                {
                    IScopedMap scopedMap;
                    if (ScopedMapDictionary.TryGetValue(k.Identifier, out scopedMap))
                    {
                        await scopedMap.RemoveKeyAsync(k.Key).ConfigureAwait(false);
                    }

                }, 
                cancellationToken);
            }

            await KeysSetMap.TryRemoveAsync(Name).ConfigureAwait(false);            
        }

        protected internal virtual Task AddKeyAsync(string identifier, string key)
        {
            if (!ScopedMapDictionary.ContainsKey(identifier)) throw new InvalidOperationException("The scoped map must be registered before changing the scope");
            return KeysSetMap.AddItemAsync(Name, new IdentifierKey() { Identifier = identifier, Key =  key});
        }

        protected internal virtual Task RemoveKeyAsync(string identifier, string key)
        {
            if (!ScopedMapDictionary.ContainsKey(identifier)) throw new InvalidOperationException("The scoped map must be registered before changing the scope");
            return KeysSetMap.TryRemoveItemAsync(Name, new IdentifierKey() { Identifier = identifier, Key = key });
        }
    }
}