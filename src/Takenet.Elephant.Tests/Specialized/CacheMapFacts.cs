using System;
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

        public override IMap<TKey, TValue> Create()
        {
            return Create(CreateSource(), CreateCache());
        }

        public virtual IMap<TKey, TValue> Create(IMap<TKey, TValue> source, IMap<TKey, TValue> cache)
        {
            return new CacheMap<TKey, TValue>(source, cache, TimeSpan.FromSeconds(60));
        }

        [Fact(DisplayName = "AddingAddsToSourceAndToTheCache")]
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

        [Fact(DisplayName = "AddingAddsToSourceAndOverwritesExistingValuesInTheCache")]
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
            await map.TryAddAsync(key1, value2, true); // Overwrite value is true, no synchronization required

            // Assert
            AssertIsTrue(await map.ContainsKeyAsync(key1));
            AssertIsTrue(await source.ContainsKeyAsync(key1));
            AssertIsTrue(await cache.ContainsKeyAsync(key1));
            AssertEquals(await map.GetValueOrDefaultAsync(key1), value2);
            AssertEquals(await source.GetValueOrDefaultAsync(key1), value2);
            AssertEquals(await cache.GetValueOrDefaultAsync(key1), value2);
        }

        [Fact(DisplayName = "AddingAddsToSourceAndOverwritesExistingValuesInTheCacheOnSynchronization")]
        public virtual async Task AddingAddsToSourceAndOverwritesExistingValuesInTheCacheOnSynchronization()
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
            await map.TryAddAsync(key1, value2, false); // Overwrite value is false, a synchronization will be required

            // Assert
            AssertIsTrue(await map.ContainsKeyAsync(key1));
            AssertIsTrue(await source.ContainsKeyAsync(key1));
            AssertIsTrue(await cache.ContainsKeyAsync(key1));
            AssertEquals(await map.GetValueOrDefaultAsync(key1), value2);
            AssertEquals(await source.GetValueOrDefaultAsync(key1), value2);
            AssertEquals(await cache.GetValueOrDefaultAsync(key1), value2);
        }


        [Fact(DisplayName = "QueryingSynchonizesTheSourceAndCache")]
        public virtual async Task QueryingSynchonizesTheSourceAndCache()
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
            var key4 = CreateKey();
            var value4 = CreateValue(key4);
            var key5 = CreateKey();
            var value5 = CreateValue(key5);
            var key6 = CreateKey();
            var value6 = CreateValue(key6);
            var key7 = CreateKey();
            var value71 = CreateValue(key7);
            var value72 = CreateValue(key7);
            var key8 = CreateKey();
            var value81 = CreateValue(key8);
            var value82 = CreateValue(key8);
            var key9 = CreateKey();
            var value91 = CreateValue(key9);
            var value92 = CreateValue(key9);

            if (!await source.TryAddAsync(key1, value1)) throw new Exception("Could not arrange the test");
            if (!await source.TryAddAsync(key2, value2)) throw new Exception("Could not arrange the test");
            if (!await source.TryAddAsync(key3, value3)) throw new Exception("Could not arrange the test");
            if (!await cache.TryAddAsync(key4, value4)) throw new Exception("Could not arrange the test");
            if (!await cache.TryAddAsync(key5, value5)) throw new Exception("Could not arrange the test");
            if (!await cache.TryAddAsync(key6, value6)) throw new Exception("Could not arrange the test");
            if (!await source.TryAddAsync(key7, value71)) throw new Exception("Could not arrange the test");
            if (!await cache.TryAddAsync(key7, value72)) throw new Exception("Could not arrange the test");
            if (!await source.TryAddAsync(key8, value81)) throw new Exception("Could not arrange the test");
            if (!await cache.TryAddAsync(key8, value82)) throw new Exception("Could not arrange the test");
            if (!await source.TryAddAsync(key9, value91)) throw new Exception("Could not arrange the test");
            if (!await cache.TryAddAsync(key9, value92)) throw new Exception("Could not arrange the test");


            // Act
            var actual1 = await map.GetValueOrDefaultAsync(key1);
            var actual2 = await map.GetValueOrDefaultAsync(key2);
            var actual3 = await map.GetValueOrDefaultAsync(key3);
            var actual4 = await map.GetValueOrDefaultAsync(key4);
            var actual5 = await map.GetValueOrDefaultAsync(key5);
            var actual6 = await map.GetValueOrDefaultAsync(key6);
            var actual7 = await map.GetValueOrDefaultAsync(key7);
            var actual8 = await map.GetValueOrDefaultAsync(key8);
            var actual9 = await map.GetValueOrDefaultAsync(key9);

            // Assert
            AssertEquals(actual1, value1);
            AssertEquals(actual2, value2);
            AssertEquals(actual3, value3);
            AssertEquals(await cache.GetValueOrDefaultAsync(key1), value1);
            AssertEquals(await cache.GetValueOrDefaultAsync(key2), value2);
            AssertEquals(await cache.GetValueOrDefaultAsync(key3), value3);
            AssertEquals(default(TValue), actual4);
            AssertEquals(default(TValue), actual5);
            AssertEquals(default(TValue), actual6);
            AssertIsFalse(await cache.ContainsKeyAsync(key4));
            AssertIsFalse(await cache.ContainsKeyAsync(key5));
            AssertIsFalse(await cache.ContainsKeyAsync(key6));
            AssertEquals(actual7, value71);
            AssertEquals(actual8, value81);
            AssertEquals(actual9, value91);
            AssertEquals(await cache.GetValueOrDefaultAsync(key7), value71);
            AssertEquals(await cache.GetValueOrDefaultAsync(key8), value81);
            AssertEquals(await cache.GetValueOrDefaultAsync(key9), value91);
        }

        [Fact(DisplayName = "RemovingRemovesFromTheSourceAndCache")]
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

    }
}
