using System.Linq;
using System.Threading.Tasks;
using AutoFixture;
using NFluent;
using Take.Elephant.Specialized.Cache;
using Xunit;

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
                actual != null &&
                expected != null)
            {
                var actualSet = (ISet<TValue>) actual;
                var expectedSet = (ISet<TValue>) expected;
                base.AssertCollectionEquals(actualSet, expectedSet);
            }
            else
            {
                base.AssertEquals(actual, expected);
            }
        }

        public abstract ISet<TValue> CreateValue(TKey key, bool populate);

        public virtual TValue CreateItem()
        {
            return Fixture.Create<TValue>();
        }

        public override IMap<TKey, ISet<TValue>> Create(IMap<TKey, ISet<TValue>> source, IMap<TKey, ISet<TValue>> cache,
            IBus<string, SynchronizationEvent<TKey>> synchronizationBus, string synchronizationChannel)
        {
            return new DistributedCacheSetMap<TKey, TValue>((ISetMap<TKey, TValue>) source,
                (ISetMap<TKey, TValue>) cache, synchronizationBus, synchronizationChannel);
        }

        [Fact(DisplayName = nameof(AddToSetShouldSynchronizeTheCache))]
        public async Task AddToSetShouldSynchronizeTheCache()
        {
            // Arrange
            var source = CreateSource();
            var cache = CreateCache();
            var synchronizationBus = CreateSynchronizationBus();
            var synchronizationChannel = CreateSynchronizationChannel();
            var target1 = (ISetMap<TKey, TValue>) Create(source, cache, synchronizationBus, synchronizationChannel);
            var target2 = (ISetMap<TKey, TValue>) Create(source, cache, synchronizationBus, synchronizationChannel);
            var target3 = (ISetMap<TKey, TValue>) Create(source, cache, synchronizationBus, synchronizationChannel);
            var key = CreateKey();
            var value1 = CreateValue(key);
            await target1.TryAddAsync(key, value1, true);
            await target1.GetValueOrDefaultAsync(key); // Force the caching of values
            await target2.GetValueOrDefaultAsync(key);
            await target3.GetValueOrDefaultAsync(key);
            var item1 = CreateItem();
            var item2 = CreateItem();
            var item3 = CreateItem();
            var value1Copy = await value1.ToMemorySetAsync();
            await value1Copy.AddRangeAsync(new[] {item1, item2, item3}.ToAsyncEnumerable());

            // Act
            await target1.AddItemAsync(key, item1);
            await target2.AddItemAsync(key, item2);
            await target3.AddItemAsync(key, item3);

            // Assert
            var actual1 = await target1.GetValueOrDefaultAsync(key);
            var actual2 = await target2.GetValueOrDefaultAsync(key);
            var actual3 = await target3.GetValueOrDefaultAsync(key);
            AssertEquals(actual1, value1Copy);
            AssertEquals(actual2, value1Copy);
            AssertEquals(actual3, value1Copy);
        }
    }
}