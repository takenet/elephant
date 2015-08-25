using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Takenet.Elephant.Specialized;
using Takenet.Elephant.Specialized.Scoping;
using Xunit;

namespace Takenet.Elephant.Tests.Specialized
{
    public abstract class CacheMapFacts<TKey, TValue> : MapFacts<TKey, TValue>
    {
        public abstract IMap<TKey, TValue> CreateSource();

        public abstract IMap<TKey, TValue> CreateCache();

        public abstract ISynchronizer<IMap<TKey, TValue>> CreateSynchronizer();

        public override IMap<TKey, TValue> Create()
        {
            return Create(CreateSource(), CreateCache());
        }

        public IMap<TKey, TValue> Create(IMap<TKey, TValue> source, IMap<TKey, TValue> cache)
        {
            return new CacheMap<TKey, TValue>(source, cache, CreateSynchronizer());
        }

        [Fact(DisplayName = "AddingToSourceAlsoAddsToTheCache")]
        public virtual async Task AddingToSourceAlsoAddsToTheCache()
        {
            // Arrange
            var source = CreateSource();
            var cache = CreateCache();
            var map = Create(source, cache);
            var key1 = CreateKey();
            var value1 = CreateValue(key1);
            var key2 = CreateKey();
            var value2 = CreateValue(key2);
            var key3 = CreateKey();
            var value3 = CreateValue(key3);

            // Act
            await map.TryAddAsync(key1, value1);
            await map.TryAddAsync(key2, value2);
            await map.TryAddAsync(key3, value3);

            // Assert
            AssertIsTrue(await source.ContainsKeyAsync(key1));
            AssertIsTrue(await cache.ContainsKeyAsync(key1));
            AssertEquals(await source.GetValueOrDefaultAsync(key1), value1);
            AssertEquals(await cache.GetValueOrDefaultAsync(key1), value1);

            AssertIsTrue(await source.ContainsKeyAsync(key2));
            AssertIsTrue(await cache.ContainsKeyAsync(key2));
            AssertEquals(await source.GetValueOrDefaultAsync(key2), value2);
            AssertEquals(await cache.GetValueOrDefaultAsync(key2), value2);

            AssertIsTrue(await source.ContainsKeyAsync(key3));
            AssertIsTrue(await cache.ContainsKeyAsync(key3));
            AssertEquals(await source.GetValueOrDefaultAsync(key3), value3);
            AssertEquals(await cache.GetValueOrDefaultAsync(key3), value3);
        }
    }
}
