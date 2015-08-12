namespace Takenet.Elephant.Specialized.Scoping
{
    public class ScopedMapFactory : IScopedMapFactory
    {
        public IMap<TKey, TValue> Create<TKey, TValue>(IMap<TKey, TValue> map, IScope scope, ISerializer<TKey> keySerializer)
        {
            return new ScopedMap<TKey, TValue>(map, scope, keySerializer);
        }

        public ISetMap<TKey, TValue> Create<TKey, TValue>(ISetMap<TKey, TValue> map, IScope scope, ISerializer<TKey> keySerializer)
        {
            return new ScopedSetMap<TKey, TValue>(map, scope, keySerializer);
        }

    }
}