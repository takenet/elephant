using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using NFluent;
using Ploeh.AutoFixture;
using Xunit;
using Xunit.Extensions;

namespace Takenet.SimplePersistence.Tests
{
    public abstract class MapFacts<TKey, TValue>
    {
        protected readonly Fixture Fixture;

        protected MapFacts()
        {
            Fixture = new Fixture();
        }

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
            Check.That(actual).IsTrue();
            Check.That(await map.GetValueOrDefaultAsync(key)).IsEqualTo(value);
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
            Check.That(actual).IsTrue();
            Check.That(await map.GetValueOrDefaultAsync(key)).IsEqualTo(newValue);
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
            Check.That(actual).IsFalse();
            Check.That(await map.GetValueOrDefaultAsync(key)).IsEqualTo(value);
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
            Check.That(actual).IsEqualTo(value);
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
            Check.That(actual).IsEqualTo(default(TValue));
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
            Check.That(actual).IsTrue();
            Check.That(await map.GetValueOrDefaultAsync(key)).IsEqualTo(default(TValue));
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
            Check.That(actual).IsFalse();
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
            Check.That(actual).IsTrue();
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
            Check.That(actual).IsFalse();
        }
    }
}
