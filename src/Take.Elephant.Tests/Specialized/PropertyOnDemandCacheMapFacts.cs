using System;
using System.Threading.Tasks;
using Take.Elephant.Specialized.Cache;
using Xunit;

namespace Take.Elephant.Tests.Specialized
{
    public abstract class PropertyOnDemandCacheMapFacts<TKey, TValue, TProperty> : PropertyMapFacts<TKey, TValue, TProperty>
    {
        public abstract IPropertyMap<TKey, TValue> CreateSource();

        public abstract IPropertyMap<TKey, TValue> CreateCache();

        public override IPropertyMap<TKey, TValue> Create()
        {
            return Create(CreateSource(), CreateCache(), TimeSpan.FromMinutes(30));
        }
        
        public virtual OnDemandCacheMap<TKey, TValue> Create(IMap<TKey, TValue> source, IMap<TKey, TValue> cache, TimeSpan cacheExpiration = default)
        {
            return new OnDemandCacheMap<TKey, TValue>(source, cache, cacheExpiration);
        }

        [Fact(DisplayName = nameof(SetPropertyOfExistingKeyShouldUpdateSourceAndCache))]
        public virtual async Task SetPropertyOfExistingKeyShouldUpdateSourceAndCache()
        {
            // Arrange
            var source = CreateSource();
            var cache = CreateCache();
            var map = Create(source, cache);
            var key = CreateKey();
            var value = CreateValue(key);
            var property = CreateProperty();
            if (!await map.TryAddAsync(key, value)) throw new Exception("Could not arrange the test");

            // Act
            await map.SetPropertyValueAsync(key, property.Key, property.Value);

            // Assert
            var sourceActual = await source.GetPropertyValueOrDefaultAsync<TProperty>(key, property.Key);
            AssertEquals(sourceActual, property.Value);
            var cacheActual = await cache.GetPropertyValueOrDefaultAsync<TProperty>(key, property.Key);
            AssertEquals(cacheActual, property.Value);
        }
        
        [Fact(DisplayName = nameof(SetPropertyOfNonExistingKeyShouldUpdateSourceAndCache))]
        public virtual async Task SetPropertyOfNonExistingKeyShouldUpdateSourceAndCache()
        {
            // Arrange
            var source = CreateSource();
            var cache = CreateCache();
            var map = Create(source, cache);
            var key = CreateKey();
            var property = CreateProperty();

            // Act
            await map.SetPropertyValueAsync(key, property.Key, property.Value);

            // Assert
            var sourceActual = await source.GetPropertyValueOrDefaultAsync<TProperty>(key, property.Key);
            AssertEquals(sourceActual, property.Value);
            var cacheActual = await cache.GetPropertyValueOrDefaultAsync<TProperty>(key, property.Key);
            AssertEquals(cacheActual, property.Value);
        }
    }
}