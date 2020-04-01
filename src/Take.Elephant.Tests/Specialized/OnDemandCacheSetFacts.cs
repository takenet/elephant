using System;
using System.Linq;
using System.Threading.Tasks;
using Take.Elephant.Specialized;
using Take.Elephant.Specialized.Cache;
using Xunit;

namespace Take.Elephant.Tests.Specialized
{

    public abstract class OnDemandCacheSetFacts<T> : SetFacts<T>
    {
        public abstract ISet<T> CreateSource();

        public abstract ISet<T> CreateCache();

        public override ISet<T> Create()
        {
            return Create(CreateSource(), CreateCache());
        }

        public virtual ISet<T> Create(ISet<T> source, ISet<T> cache)
        {
            return new OnDemandCacheSet<T>(source, cache);
        }

        [Fact(DisplayName = "AddingAddsToSourceAndCache")]
        public virtual async Task AddingAddsToSourceAndCache()
        {
            // Arrange
            var source = CreateSource();
            var cache = CreateCache();
            var set = Create(source, cache);
            var item1 = CreateItem();
            var item2 = CreateItem();
            var item3 = CreateItem();

            // Act
            await set.AddAsync(item1);
            await set.AddAsync(item2);

            // Assert
            AssertIsTrue(await set.ContainsAsync(item1));
            AssertIsTrue(await set.ContainsAsync(item2));
            AssertIsFalse(await set.ContainsAsync(item3));
            AssertIsTrue(await source.ContainsAsync(item1));
            AssertIsTrue(await source.ContainsAsync(item2));
            AssertIsFalse(await source.ContainsAsync(item3));
            AssertIsTrue(await cache.ContainsAsync(item1));
            AssertIsTrue(await cache.ContainsAsync(item2));
            AssertIsFalse(await cache.ContainsAsync(item3));

        }

        [Fact(DisplayName = "RemovingRemovesFromSourceAndCache")]
        public virtual async Task RemovingRemovesFromSourceAndCache()
        {
            // Arrange
            var source = CreateSource();
            var cache = CreateCache();
            var set = Create(source, cache);
            var item1 = CreateItem();
            var item2 = CreateItem();
            var item3 = CreateItem();
            await set.AddAsync(item1);
            await set.AddAsync(item2);
            await set.AddAsync(item3);

            // Act
            var actual = await set.TryRemoveAsync(item1);
            actual = actual && await set.TryRemoveAsync(item2);

            // Assert
            AssertIsTrue(actual);
            AssertIsFalse(await set.ContainsAsync(item1));
            AssertIsFalse(await set.ContainsAsync(item2));
            AssertIsTrue(await set.ContainsAsync(item3));
            AssertIsFalse(await source.ContainsAsync(item1));
            AssertIsFalse(await source.ContainsAsync(item2));
            AssertIsTrue(await source.ContainsAsync(item3));
            AssertIsFalse(await cache.ContainsAsync(item1));
            AssertIsFalse(await cache.ContainsAsync(item2));
            AssertIsTrue(await cache.ContainsAsync(item3));
        }

        [Fact(DisplayName = "EnumerateExistingItemsOnSourceSucceeds")]
        public virtual async Task EnumerateExistingItemsOnSourceSucceeds()
        {
            // Arrange
            var source = CreateSource();
            var cache = CreateCache();
            var set = Create(source, cache);
            var item1 = CreateItem();
            var item2 = CreateItem();
            var item3 = CreateItem();
            await source.AddAsync(item1);
            await source.AddAsync(item2);
            await source.AddAsync(item3);

            // Act
            var result = await set.AsEnumerableAsync();

            // Assert
            AssertEquals(await result.CountAsync(), 3);
            AssertEquals(await set.GetLengthAsync(), 3);
            AssertIsTrue(await result.ContainsAsync(item1));
            AssertIsTrue(await result.ContainsAsync(item2));
            AssertIsTrue(await result.ContainsAsync(item3));
        }

        [Fact(DisplayName = "EnumerateAfterRemovingItemsSucceeds", Skip = "The on demand cache do not returns a trully enumerable instance, but a cached one.")]
        public override async Task EnumerateAfterRemovingItemsSucceeds()
        {

        }
    }
}