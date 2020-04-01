using System.Linq;
using System.Threading.Tasks;
using AutoFixture;
using Xunit;

namespace Take.Elephant.Tests
{
    public abstract class MapFacts<TKey, TValue> : FactsBase
    {
        public abstract IMap<TKey, TValue> Create();

        public virtual TKey CreateKey()
        {
            return Fixture.Create<TKey>();
        }

        public virtual TValue CreateValue(TKey key)
        {
            return Fixture.Create<TValue>();
        }

        [Fact(DisplayName = "AddNewKeyAndValueSucceeds")]
        public virtual async Task AddNewKeyAndValueSucceeds()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var value = CreateValue(key);

            // Act
            var actual = await map.TryAddAsync(key, value, false);

            // Assert
            AssertIsTrue(actual);
            AssertEquals(await map.GetValueOrDefaultAsync(key), value);
        }

        [Fact(DisplayName = "OverwriteExistingKeySucceeds")]
        public virtual async Task OverwriteExistingKeySucceeds()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var value = CreateValue(key);
            await map.TryAddAsync(key, value, false);
            value = await map.GetValueOrDefaultAsync(key);

            // Act
            var actual = await map.TryAddAsync(key, value, true);

            // Assert            
            AssertIsTrue(actual);
            AssertEquals(await map.GetValueOrDefaultAsync(key), value);
        }

        [Fact(DisplayName = "OverwriteExistingKeyAndValueSucceeds")]
        public virtual async Task OverwriteExistingKeyAndValueSucceeds()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var value = CreateValue(key);
            await map.TryAddAsync(key, value, false);
            var newValue = CreateValue(key);

            // Act
            var actual = await map.TryAddAsync(key, newValue, true);

            // Assert            
            AssertIsTrue(actual);
            var storedValue = await map.GetValueOrDefaultAsync(key);
            AssertEquals(storedValue, newValue);
        }

        [Fact(DisplayName = nameof(AddExistingKeyReturnsFalse))]
        public virtual async Task AddExistingKeyReturnsFalse()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var value = CreateValue(key);
            await map.TryAddAsync(key, value, false);
            value = await map.GetValueOrDefaultAsync(key);

            // Act
            var actual = await map.TryAddAsync(key, value, false);

            // Assert
            AssertIsFalse(actual);
            var storedValue = await map.GetValueOrDefaultAsync(key);
            AssertEquals(storedValue, value);
        }
        
        [Fact(DisplayName = nameof(AddExistingKeyConcurrentlyReturnsFalse))]
        public virtual async Task AddExistingKeyConcurrentlyReturnsFalse()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var value = CreateValue(key);
            var count = 100;

            // Act
            var actuals = await
                Task.WhenAll(
                    Enumerable
                        .Range(0, count)
                        .Select(i => Task.Run(
                            () => map.TryAddAsync(key, value, false))));
            
            // Assert
            AssertEquals(actuals.Count(c => c == true), 1);
            AssertEquals(actuals.Count(c => c == false), count - 1);
            
            var storedValue = await map.GetValueOrDefaultAsync(key);
            AssertEquals(storedValue, value);
        }        

        [Fact(DisplayName = "AddExistingKeyAndValueFails")]
        public virtual async Task AddExistingKeyAndValueFails()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var value = CreateValue(key);
            await map.TryAddAsync(key, value, false);
            var newValue = CreateValue(key);

            // Act
            var actual = await map.TryAddAsync(key, newValue, false);

            // Assert
            AssertIsFalse(actual);
            var existing = await map.GetValueOrDefaultAsync(key);
            AssertEquals(existing, value);
        }

        [Fact(DisplayName = "GetExistingKeyReturnsValue")]
        public virtual async Task GetExistingKeyReturnsValue()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var value = CreateValue(key);
            await map.TryAddAsync(key, value, false);

            // Act
            var actual = await map.GetValueOrDefaultAsync(key);

            // Assert
            AssertEquals(actual, value);
        }

        [Fact(DisplayName = "GetNonExistingKeyReturnsDefault")]
        public virtual async Task GetNonExistingKeyReturnsDefault()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();

            // Act
            var actual = await map.GetValueOrDefaultAsync(key);

            // Assert
            AssertEquals(actual, default(TValue));
        }

        [Fact(DisplayName = "TryRemoveExistingKeyAndValueSucceeds")]
        public virtual async Task TryRemoveExistingKeyAndValueSucceeds()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var value = CreateValue(key);
            await map.TryAddAsync(key, value, false);

            // Act
            var actual = await map.TryRemoveAsync(key);

            // Assert
            AssertIsTrue(actual);
            AssertEquals(await map.GetValueOrDefaultAsync(key), default(TValue));
        }

        [Fact(DisplayName = "TryRemoveNonExistingKeyFails")]
        public virtual async Task TryRemoveNonExistingKeyFails()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();

            // Act
            var actual = await map.TryRemoveAsync(key);

            // Assert
            AssertIsFalse(actual);
        }

        [Fact(DisplayName = "CheckForExistingKeyReturnsTrue")]
        public virtual async Task CheckForExistingKeyReturnsTrue()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var value = CreateValue(key);
            await map.TryAddAsync(key, value, false);

            // Act
            var actual = await map.ContainsKeyAsync(key);

            // Assert
            AssertIsTrue(actual);
        }

        [Fact(DisplayName = "CheckForNonExistingKeyReturnsFalse")]
        public virtual async Task CheckForNonExistingKeyReturnsFalse()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();

            // Act
            var actual = await map.ContainsKeyAsync(key);

            // Assert
            AssertIsFalse(actual);
        }
    }
}
