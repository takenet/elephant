using System.Linq;
using NFluent;
using Take.Elephant.Specialized.Cache;

namespace Take.Elephant.Tests.Specialized
{
    public abstract class DistributedCacheSetMapFacts<TKey, TValue> : DistributedCacheMapFacts<TKey, ISet<TValue>>
    {
        public override ISet<TValue> CreateValue(TKey key)
        {
            return CreateValue(key, true);
        }
        
        public override void AssertEquals<T>(T actual, T expected)
        {
            if (typeof(ISet<TValue>).IsAssignableFrom(typeof(T)) &&
                actual != null && expected != null)
            {
                var actualSet = (ISet<TValue>) actual;
                var expectedSet = (ISet<TValue>)expected;
                Check.That(actualSet.AsEnumerableAsync().ToListAsync().Result).Contains(expectedSet.AsEnumerableAsync().ToListAsync().Result);
            }
            else
            {
                base.AssertEquals(actual, expected);
            }            
        }

        public abstract ISet<TValue> CreateValue(TKey key, bool populate);

        public override IMap<TKey, ISet<TValue>> Create(IMap<TKey, ISet<TValue>> source, IMap<TKey, ISet<TValue>> cache, IBus<string, SynchronizationEvent<TKey>> synchronizationBus, string synchronizationChannel)
        {
            return new DistributedCacheSetMap<TKey, TValue>((ISetMap<TKey,TValue>)source, (ISetMap<TKey,TValue>)cache, synchronizationBus, synchronizationChannel);
        }
    }
}