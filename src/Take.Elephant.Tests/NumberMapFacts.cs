using System;
using System.Threading.Tasks;
using Ploeh.AutoFixture;
using Xunit;

namespace Take.Elephant.Tests
{
    public abstract class NumberMapFacts<TKey> : FactsBase
    {
        public abstract INumberMap<TKey> Create();

        public virtual TKey CreateKey()
        {
            return Fixture.Create<TKey>();
        }

        public virtual long CreateValue(TKey key)
        {
            return Fixture.Create<long>();
        }

        [Fact(DisplayName = "IncrementExistingValueByOneSucceeds")]
        public virtual async Task IncrementExistingValueByOneSucceeds()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var value = CreateValue(key);
            if (!await map.TryAddAsync(key, value)) throw new Exception("Could not arrange the test");

            // Act
            var actual = await map.IncrementAsync(key);

            // Assert
            AssertEquals(actual, value + 1);
        }

        [Fact(DisplayName = "IncrementNonExistingKeyByOneSucceeds")]
        public virtual async Task IncrementNonExistingKeyByOneSucceeds()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();

            // Act
            var actual = await map.IncrementAsync(key);

            // Assert
            AssertEquals(actual, 1);
        }

        [Fact(DisplayName = "IncrementExistingValueSucceeds")]
        public virtual async Task IncrementExistingValueSucceeds()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var value = CreateValue(key);
            if (!await map.TryAddAsync(key, value)) throw new Exception("Could not arrange the test");
            var increment = CreateValue(key);

            // Act
            var actual = await map.IncrementAsync(key, increment);

            // Assert
            AssertEquals(actual, value + increment);
        }

        [Fact(DisplayName = "IncrementNonExistingKeySucceeds")]
        public virtual async Task IncrementNonExistingKeySucceeds()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var value = CreateValue(key);
            // Act
            var actual = await map.IncrementAsync(key, value);

            // Assert
            AssertEquals(actual, value);
        }

        [Fact(DisplayName = "DecrementExistingValueByOneSucceeds")]
        public virtual async Task DecrementExistingValueByOneSucceeds()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var value = CreateValue(key);
            if (!await map.TryAddAsync(key, value)) throw new Exception("Could not arrange the test");

            // Act
            var actual = await map.DecrementAsync(key);

            // Assert
            AssertEquals(actual, value - 1);
        }

        [Fact(DisplayName = "DecrementNonExistingKeyByOneSucceeds")]
        public virtual async Task DecrementNonExistingKeyByOneSucceeds()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();

            // Act
            var actual = await map.DecrementAsync(key);

            // Assert
            AssertEquals(actual, -1);
        }

        [Fact(DisplayName = "DecrementExistingValueSucceeds")]
        public virtual async Task DecrementExistingValueSucceeds()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var value = CreateValue(key);
            if (!await map.TryAddAsync(key, value)) throw new Exception("Could not arrange the test");
            var decrement = CreateValue(key);

            // Act
            var actual = await map.DecrementAsync(key, decrement);

            // Assert
            AssertEquals(actual, value - decrement);
        }

        [Fact(DisplayName = "DecrementNonExistingKeySucceeds")]
        public virtual async Task DecrementNonExistingKeySucceeds()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var value = CreateValue(key);

            // Act
            var actual = await map.DecrementAsync(key, value);

            // Assert
            AssertEquals(actual, value * -1);
        }
    }
}
