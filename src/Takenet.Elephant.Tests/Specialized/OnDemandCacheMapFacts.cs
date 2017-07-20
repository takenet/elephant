using System;
using System.Threading.Tasks;
using Takenet.Elephant.Specialized;
using Xunit;

namespace Takenet.Elephant.Tests.Specialized
{
    public abstract class OnDemandCacheMapFacts<TKey, TValue> : MapFacts<TKey, TValue>
    {
        public abstract IMap<TKey, TValue> CreateSource();

        public abstract IMap<TKey, TValue> CreateCache();

        public override IMap<TKey, TValue> Create()
        {
            return Create(CreateSource(), CreateCache(), TimeSpan.FromMinutes(30));
        }

        public virtual IMap<TKey, TValue> Create(IMap<TKey, TValue> source, IMap<TKey, TValue> cache, TimeSpan cacheExpiration = default(TimeSpan))
        {
            return new OnDemandCacheMap<TKey, TValue>(source, cache, cacheExpiration);
        }

        [Fact(DisplayName = nameof(AddingAddsToSourceAndToTheCache))]
        public virtual async Task AddingAddsToSourceAndToTheCache()
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
            var success = await map.TryAddAsync(key1, value1);
            success = success && await map.TryAddAsync(key2, value2);
            success = success && await map.TryAddAsync(key3, value3);

            // Assert
            AssertIsTrue(success);
            AssertIsTrue(await map.ContainsKeyAsync(key1));
            AssertIsTrue(await source.ContainsKeyAsync(key1));
            AssertIsTrue(await cache.ContainsKeyAsync(key1));
            AssertEquals(await map.GetValueOrDefaultAsync(key1), value1);
            AssertEquals(await source.GetValueOrDefaultAsync(key1), value1);
            AssertEquals(await cache.GetValueOrDefaultAsync(key1), value1);

            AssertIsTrue(await map.ContainsKeyAsync(key2));
            AssertIsTrue(await source.ContainsKeyAsync(key2));
            AssertIsTrue(await cache.ContainsKeyAsync(key2));
            AssertEquals(await map.GetValueOrDefaultAsync(key2), value2);
            AssertEquals(await source.GetValueOrDefaultAsync(key2), value2);
            AssertEquals(await cache.GetValueOrDefaultAsync(key2), value2);

            AssertIsTrue(await map.ContainsKeyAsync(key3));
            AssertIsTrue(await source.ContainsKeyAsync(key3));
            AssertIsTrue(await cache.ContainsKeyAsync(key3));
            AssertEquals(await map.GetValueOrDefaultAsync(key3), value3);
            AssertEquals(await source.GetValueOrDefaultAsync(key3), value3);
            AssertEquals(await cache.GetValueOrDefaultAsync(key3), value3);
        }

        [Fact(DisplayName = nameof(AddingAddsToSourceAndOverwritesExistingValuesInTheCache))]
        public virtual async Task AddingAddsToSourceAndOverwritesExistingValuesInTheCache()
        {
            // Arrange
            var source = CreateSource();
            var cache = CreateCache();
            var map = Create(source, cache);
            var key1 = CreateKey();
            var value1 = CreateValue(key1);
            var value2 = CreateValue(key1);
            if (!await cache.TryAddAsync(key1, value1)) throw new Exception("Could not arrange the test");

            // Act
            await map.TryAddAsync(key1, value2, true);

            // Assert
            AssertIsTrue(await map.ContainsKeyAsync(key1));
            AssertIsTrue(await source.ContainsKeyAsync(key1));
            AssertIsTrue(await cache.ContainsKeyAsync(key1));
            AssertEquals(await map.GetValueOrDefaultAsync(key1), value2);
            AssertEquals(await source.GetValueOrDefaultAsync(key1), value2);
            AssertEquals(await cache.GetValueOrDefaultAsync(key1), value2);
        }

        [Fact(DisplayName = nameof(RemovingRemovesFromTheSourceAndCache))]
        public virtual async Task RemovingRemovesFromTheSourceAndCache()
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
            if (!await map.TryAddAsync(key1, value1)) throw new Exception("Could not arrange the test");
            if (!await map.TryAddAsync(key2, value2)) throw new Exception("Could not arrange the test");
            if (!await map.TryAddAsync(key3, value3)) throw new Exception("Could not arrange the test");

            // Act
            var actual = await map.TryRemoveAsync(key1);

            // Assert
            AssertIsTrue(actual);
            AssertIsFalse(await map.ContainsKeyAsync(key1));
            AssertIsFalse(await source.ContainsKeyAsync(key1));
            AssertIsFalse(await cache.ContainsKeyAsync(key1));
            AssertIsTrue(await map.ContainsKeyAsync(key2));
            AssertIsTrue(await source.ContainsKeyAsync(key2));
            AssertIsTrue(await cache.ContainsKeyAsync(key2));
            AssertIsTrue(await map.ContainsKeyAsync(key3));
            AssertIsTrue(await source.ContainsKeyAsync(key3));
            AssertIsTrue(await cache.ContainsKeyAsync(key3));
        }

        [Fact(DisplayName = nameof(QueryingWithExpirationShouldExpiresInCache))]
        public virtual async Task QueryingWithExpirationShouldExpiresInCache()
        {
            // Arrange
            var expiration = TimeSpan.FromMilliseconds(100);
            var source = CreateSource();
            var cache = CreateCache();
            var map = Create(source, cache, expiration);
            var key1 = CreateKey();
            var value1 = CreateValue(key1);
            var key2 = CreateKey();
            var value2 = CreateValue(key2);

            if (!await source.TryAddAsync(key1, value1)) throw new Exception("Could not arrange the test");
            if (!await source.TryAddAsync(key2, value2)) throw new Exception("Could not arrange the test");

            // Act
            var actual1 = await map.GetValueOrDefaultAsync(key1);
            var actual2 = await map.GetValueOrDefaultAsync(key2);

            // Assert
            AssertEquals(actual1, value1);
            AssertEquals(actual2, value2);
            AssertIsTrue(await cache.ContainsKeyAsync(key1));
            AssertIsTrue(await cache.ContainsKeyAsync(key2));
            await Task.Delay(expiration);
            AssertIsFalse(await cache.ContainsKeyAsync(key1));
            AssertIsFalse(await cache.ContainsKeyAsync(key2));
        }


        [Fact(DisplayName = nameof(QueryingIfItemExistsShouldAddItToCache))]
        public virtual async Task QueryingIfItemExistsShouldAddItToCache()
        {
            // Arrange
            var source = CreateSource();
            var cache = CreateCache();
            var map = Create(source, cache);
            var key1 = CreateKey();
            var value1 = CreateValue(key1);
            var key2 = CreateKey();
            var value2 = CreateValue(key2);

            if (!await source.TryAddAsync(key1, value1)) throw new Exception("Could not arrange the test");
            if (!await source.TryAddAsync(key2, value2)) throw new Exception("Could not arrange the test");

            // Act
            var actual1 = await map.ContainsKeyAsync(key1);
            var actual2 = await map.ContainsKeyAsync(key2);

            // Assert
            AssertIsTrue(actual1);
            AssertIsTrue(actual2);
            AssertIsTrue(await cache.ContainsKeyAsync(key1));
            AssertIsTrue(await cache.ContainsKeyAsync(key2));
            AssertEquals(await cache.GetValueOrDefaultAsync(key1), value1);
            AssertEquals(await cache.GetValueOrDefaultAsync(key2), value2);
        }


        [Fact(DisplayName = nameof(AddWithExpirationShouldExpiresInCache))]
        public virtual async Task AddWithExpirationShouldExpiresInCache()
        {
            // Arrange
            var expiration = TimeSpan.FromMilliseconds(100);
            var source = CreateSource();
            var cache = CreateCache();
            var map = Create(source, cache, expiration);
            var key1 = CreateKey();
            var value1 = CreateValue(key1);
            var key2 = CreateKey();
            var value2 = CreateValue(key2);

            // Act
            if (!await map.TryAddAsync(key1, value1)) throw new Exception("Could not arrange the test");
            if (!await map.TryAddAsync(key2, value2)) throw new Exception("Could not arrange the test");
                        
            // Assert            
            AssertIsTrue(await cache.ContainsKeyAsync(key1));
            AssertIsTrue(await cache.ContainsKeyAsync(key2));
            await Task.Delay(expiration);
            AssertIsFalse(await cache.ContainsKeyAsync(key1));
            AssertIsFalse(await cache.ContainsKeyAsync(key2));
        }
    }
}
