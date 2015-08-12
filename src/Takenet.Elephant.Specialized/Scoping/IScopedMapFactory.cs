namespace Takenet.Elephant.Specialized.Scoping
{
    public interface IScopedMapFactory
    {
        IMap<TKey, TValue> Create<TKey, TValue>(IMap<TKey, TValue> map, IScope scope, ISerializer<TKey> keySerializer);

        ISetMap<TKey, TValue> Create<TKey, TValue>(ISetMap<TKey, TValue> map, IScope scope, ISerializer<TKey> keySerializer);
    }
}