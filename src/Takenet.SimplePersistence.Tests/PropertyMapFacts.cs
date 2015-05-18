using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Ploeh.AutoFixture;
using Xunit;

namespace Takenet.SimplePersistence.Tests
{
    public abstract class PropertyMapFacts<TKey, TValue, TProperty> : FactsBase
    {

        public abstract IPropertyMap<TKey, TValue> Create();

        public virtual TKey CreateKey()
        {
            return Fixture.Create<TKey>();
        }

        public virtual TValue CreateValue(TKey key)
        {
            return Fixture.Create<TValue>();
        }

        public virtual KeyValuePair<string, TProperty> CreateProperty()
        {
            var propertyInfo = typeof(TValue)                
                .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .FirstOrDefault(p => p.PropertyType == typeof (TProperty));

            if (propertyInfo == null)
                throw new ArgumentException($"The type '{typeof (TValue)}' doesn't contains a property of the type '{typeof(TProperty)}");

            return new KeyValuePair<string, TProperty>(propertyInfo.Name, Fixture.Create<TProperty>());
        }
  
        [Fact(DisplayName = "SetPropertyOfExistingKeySucceeds")]
        public async Task SetPropertyOfExistingKeySucceeds()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var value = CreateValue(key);
            var property = CreateProperty();
            await map.TryAddAsync(key, value, false);

            // Act
            await map.SetPropertyValueAsync(key, property.Key, property.Value);

            // Assert
            var actual = await map.GetPropertyValueOrDefaultAsync<TProperty>(key, property.Key);
            AssertEquals(actual, property.Value);
        }

        [Fact(DisplayName = "SetPropertyOfNonExistingKeyCreatesValueAndSucceeds")]
        public async Task SetPropertyOfNonExistingKeyCreatesValueAndSucceeds()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var property = CreateProperty();

            // Act
            await map.SetPropertyValueAsync(key, property.Key, property.Value);

            // Assert
            var actual = await map.GetPropertyValueOrDefaultAsync<TProperty>(key, property.Key);
            AssertEquals(actual, property.Value);
        }

        [Fact(DisplayName = "SetInvalidPropertyNameThrowsArgumentException")]
        public async Task SetInvalidPropertyNameThrowsArgumentException()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var property = CreateProperty();
            var propertyName = Fixture.Create<string>();

            // Act
            await Assert.ThrowsAsync<ArgumentException>(() =>
                map.SetPropertyValueAsync(key, propertyName, property.Value));
        }

        [Fact(DisplayName = "GetInvalidPropertyNameThrowsArgumentException")]
        public async Task GetInvalidPropertyNameThrowsArgumentException()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var propertyName = Fixture.Create<string>();

            // Act
            await Assert.ThrowsAsync<ArgumentException>(() =>
                map.GetPropertyValueOrDefaultAsync<TProperty>(key, propertyName));
        }

        [Fact(DisplayName = "MergeWithExistingValueSucceeds")]
        public async Task MergeWithExistingValueSucceeds()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var value = CreateValue(key);            
            await map.TryAddAsync(key, value, false);
            var property = CreateProperty();
            var clonedValue = value.Clone();
            var changedProperty = CreateProperty();
            typeof(TValue).GetProperty(property.Key).SetValue(clonedValue, changedProperty.Value);

            // Act
            await map.MergeAsync(key, clonedValue);

            // Assert
            var actual = await map.GetPropertyValueOrDefaultAsync<TProperty>(key, property.Key);
            AssertEquals(actual, changedProperty.Value);
        }

        [Fact(DisplayName = "MergeWithNonExistingValueSucceeds")]
        public async Task MergeWithNonExistingValueSucceeds()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var value = CreateValue(key);
            var property = CreateProperty();
            var clonedValue = value.Clone();
            var changedProperty = CreateProperty();
            typeof(TValue).GetProperty(property.Key).SetValue(clonedValue, changedProperty.Value);

            // Act
            await map.MergeAsync(key, clonedValue);

            // Assert
            var actual = await map.GetPropertyValueOrDefaultAsync<TProperty>(key, property.Key);
            AssertEquals(actual, changedProperty.Value);
        }
    }
}
