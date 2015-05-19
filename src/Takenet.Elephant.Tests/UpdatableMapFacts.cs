using System.Threading.Tasks;
using Ploeh.AutoFixture;
using Xunit;

namespace Takenet.Elephant.Tests
{
    public abstract class UpdatableMapFacts<TKey, TValue> : FactsBase
    {
        public abstract IUpdatableMap<TKey, TValue> Create();

        public virtual TKey CreateKey()
        {
            return Fixture.Create<TKey>();
        }

        public virtual TValue CreateValue(TKey key)
        {
            return Fixture.Create<TValue>();
        }

        [Fact(DisplayName = "UpdateExistingValueSucceeds")]
        public async Task UpdateExistingValueSucceeds()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();            
            var oldValue = CreateValue(key);            
            await map.TryAddAsync(key, oldValue, false);            
            var newValue = CreateValue(key);

            // Act
            var actual = await map.TryUpdateAsync(key, newValue, oldValue);

            // Assert
            AssertIsTrue(actual);
            var actualValue = await map.GetValueOrDefaultAsync(key);
            AssertEquals(actualValue, newValue);            
        }

        [Fact(DisplayName = "UpdateNonExistingValueFails")]
        public async Task UpdateNonExistingValueFails()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var oldValue = CreateValue(key);
            var anyValue = CreateValue(key);
            await map.TryAddAsync(key, anyValue, false);
            var newValue = CreateValue(key);

            // Act
            var actual = await map.TryUpdateAsync(key, newValue, oldValue);

            // Assert
            AssertIsFalse(actual);
            var actualValue = await map.GetValueOrDefaultAsync(key);
            AssertEquals(actualValue, anyValue);
        }
    }
}