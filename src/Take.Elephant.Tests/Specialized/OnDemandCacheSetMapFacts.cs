using System;
using System.Linq;
using System.Threading.Tasks;
using AutoFixture;
using Take.Elephant.Specialized.Cache;
using Xunit;

namespace Take.Elephant.Tests.Specialized
{
    public abstract class OnDemandCacheSetMapFacts<TKey, TValue> : OnDemandCacheMapFacts<TKey, ISet<TValue>>
    {
        public override OnDemandCacheMap<TKey, ISet<TValue>> Create(IMap<TKey, ISet<TValue>> source, IMap<TKey, ISet<TValue>> cache, TimeSpan cacheExpiration = default)
        {
            return new OnDemandCacheSetMap<TKey, TValue>((ISetMap<TKey,TValue>)source, (ISetMap<TKey, TValue>)cache, cacheExpiration);
        }

        public override void AssertEquals<T>(T actual, T expected)
        {
            if (typeof(ISet<TValue>).IsAssignableFrom(typeof(T)) &&
                actual != null && expected != null)
            {
                var actualSet = (ISet<TValue>)actual;
                var expectedSet = (ISet<TValue>)expected;

                base.AssertCollectionEquals(actualSet, expectedSet);
            }
            else
            {
                base.AssertEquals(actual, expected);
            }
        }

        public virtual TValue CreateItem()
        {
            return Fixture.Create<TValue>();
        }

        public override ISet<TValue> CreateValue(TKey key)
        {
            return CreateValue(key, true);
        }

        public abstract ISet<TValue> CreateValue(TKey key, bool populate);


        [Fact(DisplayName = "AddMultipleSetsSucceeds")]
        public virtual async Task AddMultipleSetsSucceeds()
        {
            // Arrange
            var map = Create();
            var key1 = CreateKey();
            var set1 = CreateValue(key1, false);
            var item11 = CreateItem();
            var item12 = CreateItem();
            var item13 = CreateItem();
            var item14 = CreateItem();
            var item15 = CreateItem();
            await set1.AddAsync(item11);
            await set1.AddAsync(item12);
            await set1.AddAsync(item13);
            await set1.AddAsync(item14);
            await set1.AddAsync(item15);
            var key2 = CreateKey();
            var set2 = CreateValue(key2, false);
            var item21 = CreateItem();
            var item22 = CreateItem();
            var item23 = CreateItem();
            await set2.AddAsync(item21);
            await set2.AddAsync(item22);
            await set2.AddAsync(item23);
            var key3 = CreateKey();
            var set3 = CreateValue(key3, false);
            var item31 = CreateItem();
            await set3.AddAsync(item31);

            // Act
            var actual1 = await map.TryAddAsync(key1, set1, false);
            var actual2 = await map.TryAddAsync(key2, set2, false);
            var actual3 = await map.TryAddAsync(key3, set3, false);

            // Assert
            AssertIsTrue(actual1);
            AssertIsTrue(actual2);
            AssertIsTrue(actual3);
            var actualSet1 = await map.GetValueOrDefaultAsync(key1);
            var actualSet2 = await map.GetValueOrDefaultAsync(key2);
            var actualSet3 = await map.GetValueOrDefaultAsync(key3);
            AssertIsNotNull(actualSet1);
            AssertIsNotNull(actualSet2);
            AssertIsNotNull(actualSet3);
            AssertEquals(actualSet1, set1);
            AssertEquals(actualSet2, set2);
            AssertEquals(actualSet3, set3);
            var enumerableActualSet1 = actualSet1.AsEnumerableAsync();
            var enumerableActualSet2 = actualSet2.AsEnumerableAsync();
            var enumerableActualSet3 = actualSet3.AsEnumerableAsync();
            AssertEquals(await enumerableActualSet1.CountAsync(), 5);
            AssertEquals(await actualSet1.GetLengthAsync(), 5);
            AssertEquals(await enumerableActualSet2.CountAsync(), 3);
            AssertEquals(await actualSet2.GetLengthAsync(), 3);
            AssertEquals(await enumerableActualSet3.CountAsync(), 1);
            AssertEquals(await actualSet3.GetLengthAsync(), 1);
            AssertIsTrue(await actualSet1.ContainsAsync(item11));
            AssertIsTrue(await actualSet1.ContainsAsync(item12));
            AssertIsTrue(await actualSet1.ContainsAsync(item13));
            AssertIsTrue(await actualSet1.ContainsAsync(item14));
            AssertIsTrue(await actualSet1.ContainsAsync(item15));
            AssertIsFalse(await actualSet1.ContainsAsync(item21));
            AssertIsFalse(await actualSet1.ContainsAsync(item22));
            AssertIsFalse(await actualSet1.ContainsAsync(item23));
            AssertIsFalse(await actualSet1.ContainsAsync(item31));
            AssertIsFalse(await actualSet2.ContainsAsync(item11));
            AssertIsFalse(await actualSet2.ContainsAsync(item12));
            AssertIsFalse(await actualSet2.ContainsAsync(item13));
            AssertIsFalse(await actualSet2.ContainsAsync(item14));
            AssertIsFalse(await actualSet2.ContainsAsync(item15));
            AssertIsTrue(await actualSet2.ContainsAsync(item21));
            AssertIsTrue(await actualSet2.ContainsAsync(item22));
            AssertIsTrue(await actualSet2.ContainsAsync(item23));
            AssertIsFalse(await actualSet2.ContainsAsync(item31));
            AssertIsFalse(await actualSet3.ContainsAsync(item11));
            AssertIsFalse(await actualSet3.ContainsAsync(item12));
            AssertIsFalse(await actualSet3.ContainsAsync(item13));
            AssertIsFalse(await actualSet3.ContainsAsync(item14));
            AssertIsFalse(await actualSet3.ContainsAsync(item15));
            AssertIsFalse(await actualSet3.ContainsAsync(item21));
            AssertIsFalse(await actualSet3.ContainsAsync(item22));
            AssertIsFalse(await actualSet3.ContainsAsync(item23));
            AssertIsTrue(await actualSet3.ContainsAsync(item31));
        }

        [Fact(DisplayName = "AddMultipleItemsSucceeds")]
        public virtual async Task AddMultipleItemsSucceeds()
        {
            // Arrange
            var map = (ISetMap<TKey, TValue>)Create();
            var key = CreateKey();
            var item1 = CreateItem();
            var item2 = CreateItem();
            var item3 = CreateItem();

            // Act
            await map.AddItemAsync(key, item1);
            await map.AddItemAsync(key, item2);

            // Assert
            AssertIsTrue(await map.ContainsItemAsync(key, item1));
            AssertIsTrue(await map.ContainsItemAsync(key, item2));
            AssertIsFalse(await map.ContainsItemAsync(key, item3));
        }
        
        [Fact(DisplayName = "RemoveMultipleItemsSucceeds")]
        public virtual async Task RemoveMultipleItemsSucceeds()
        {
            // Arrange
            var map = (ISetMap<TKey, TValue>)Create();
            var key = CreateKey();
            var item1 = CreateItem();
            var item2 = CreateItem();
            var item3 = CreateItem();
            await map.AddItemAsync(key, item1);
            await map.AddItemAsync(key, item2);
            await map.AddItemAsync(key, item3);

            // Act
            var actual = await map.TryRemoveItemAsync(key, item1);
            actual = actual && await map.TryRemoveItemAsync(key, item2);

            // Assert
            AssertIsTrue(actual);
            AssertIsFalse(await map.ContainsItemAsync(key, item1));
            AssertIsFalse(await map.ContainsItemAsync(key, item2));
            AssertIsTrue(await map.ContainsItemAsync(key, item3));
        }
        
        [Fact(DisplayName = nameof(AddItemBeforeCacheSynchronizationShouldRetrieveFromSource))]
        public virtual async Task AddItemBeforeCacheSynchronizationShouldRetrieveFromSource()
        {
            // Arrange
            var source = (ISetMap<TKey, TValue>)CreateSource();
            var cache1 = (ISetMap<TKey, TValue>)CreateCache();
            var cache2 = (ISetMap<TKey, TValue>)CreateCache();
            var target1 = (ISetMap<TKey, TValue>)Create(source, cache1);
            var target2 = (ISetMap<TKey, TValue>)Create(source, cache2);
            var key = CreateKey();
            var item1 = CreateItem();
            var item2 = CreateItem();
            var item3 = CreateItem();
            await source.AddItemsAsync(key, new[] {item1, item2}.ToAsyncEnumerable());

            // Act
            await target1.AddItemAsync(key, item3);
            
            // Assert
            var actual1 = await target1.GetValueOrEmptyAsync(key);
            var actual2 = await target2.GetValueOrEmptyAsync(key);
            AssertEquals(actual1, actual2);
        }
    }
}