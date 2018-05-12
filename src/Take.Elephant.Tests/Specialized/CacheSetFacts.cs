using System;
using System.Threading.Tasks;
using Take.Elephant.Specialized;
using Xunit;

namespace Take.Elephant.Tests.Specialized
{
    public abstract class CacheSetFacts<T> : SetFacts<T>
    {
        public abstract ISet<T> CreateSource();

        public abstract ISet<T> CreateCache();

        public override ISet<T> Create()
        {
            return Create(CreateSource(), CreateCache());
        }

        public virtual ISet<T> Create(ISet<T> source, ISet<T> cache)
        {
            return new CacheSet<T>(source, cache, TimeSpan.FromSeconds(60));
        }

        [Fact(DisplayName = "AddingAddsToSourceAndCache")]
        public virtual async Task AddingAddsToSourceAndCache()
        {
            // Arrange
            var source = CreateSource();
            var cache = CreateCache();
            var map = Create(source, cache);
            var item1 = CreateItem();
            var item2 = CreateItem();
            var item3 = CreateItem();

            // Act
            await map.AddAsync(item1);
            await map.AddAsync(item2);

            // Assert
            AssertIsTrue(await map.ContainsAsync(item1));
            AssertIsTrue(await map.ContainsAsync(item2));
            AssertIsFalse(await map.ContainsAsync(item3));
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
            var map = Create(source, cache);
            var item1 = CreateItem();
            var item2 = CreateItem();
            var item3 = CreateItem();
            await map.AddAsync(item1);
            await map.AddAsync(item2);
            await map.AddAsync(item3);

            // Act
            var actual = await map.TryRemoveAsync(item1);
            actual = actual && await map.TryRemoveAsync(item2);

            // Assert
            AssertIsTrue(actual);
            AssertIsFalse(await map.ContainsAsync(item1));
            AssertIsFalse(await map.ContainsAsync(item2));
            AssertIsTrue(await map.ContainsAsync(item3));
            AssertIsFalse(await source.ContainsAsync(item1));
            AssertIsFalse(await source.ContainsAsync(item2));
            AssertIsTrue(await source.ContainsAsync(item3));
            AssertIsFalse(await cache.ContainsAsync(item1));
            AssertIsFalse(await cache.ContainsAsync(item2));
            AssertIsTrue(await cache.ContainsAsync(item3));

        }
    }
}