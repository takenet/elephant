namespace Takenet.Elephant.Specialized.Scoping
{
    public class ScopedSetMap<TKey, TValue> : ScopedMap<TKey, ISet<TValue>>, ISetMap<TKey, TValue>
    {
        public ScopedSetMap(IMap<TKey, ISet<TValue>> map, IScope scope, ISerializer<TKey> keySerializer) 
            : base(map, scope, keySerializer)
        {
        }
    }
}