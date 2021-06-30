using System;
using System.Threading.Tasks;
using Xunit;
using AutoFixture;
using Shouldly;

namespace Take.Elephant.Tests
{
    public abstract class ExpirableKeyMapFacts<TKey, TValue> : FactsBase
    {
        public abstract IExpirableKeyMap<TKey, TValue> Create();

        public virtual TKey CreateKey()
        {
            return Fixture.Create<TKey>();
        }

        public virtual TValue CreateValue(TKey key)
        {
            return Fixture.Create<TValue>();
        }

        public virtual TimeSpan CreateTtl()
        {
            return TimeSpan.FromMilliseconds(500);
        }

        [Fact(DisplayName = "ExpireExistingKeyByRelativeTtlSucceeds")]
        public async Task ExpireExistingKeyByRelativeTtlSucceeds()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var value = CreateValue(key);
            if (!await map.TryAddAsync(key, value, false)) throw new Exception("Could not arrange the test");
            var ttl = CreateTtl();

            // Act
            await map.SetRelativeKeyExpirationAsync(key, ttl);
            await Task.Delay(ttl + ttl);

            // Assert
            var contains = await map.ContainsKeyAsync(key);
            AssertIsFalse(contains);
        }

        [Fact(DisplayName = "ExpireExistingKeyByAbsoluteExpirationDateSucceeds")]
        public async Task ExpireExistingKeyByAbsoluteExpirationDateSucceeds()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var value = CreateValue(key);
            if (!await map.TryAddAsync(key, value, false)) throw new Exception("Could not arrange the test");
            var ttl = CreateTtl();
            var expiration = DateTimeOffset.UtcNow.Add(ttl);

            // Act
            await map.SetAbsoluteKeyExpirationAsync(key, expiration);
            await Task.Delay(ttl * 2);

            // Assert
            var contains = await map.ContainsKeyAsync(key);
            AssertIsFalse(contains);
        }

        [Fact(DisplayName = "UpdateKeyTtlSucceeds")]
        public async Task UpdateKeyTtlSucceeds()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var value = CreateValue(key);
            if (!await map.TryAddAsync(key, value, false)) throw new Exception("Could not arrange the test");
            var ttl = CreateTtl();
            await map.SetRelativeKeyExpirationAsync(key, ttl);

            // Act
            var newTtl = ttl * 3;
            await map.SetRelativeKeyExpirationAsync(key, newTtl);

            // Assert
            await Task.Delay(ttl);
            var actual = await map.GetValueOrDefaultAsync(key);
            AssertEquals(actual, value);
            await Task.Delay(newTtl);
            var contains = await map.ContainsKeyAsync(key);
            AssertIsFalse(contains);
        }

        [Fact(DisplayName = "UpdateKeyExpirationDateSucceeds")]
        public async Task UpdateKeyExpirationDateSucceeds()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var value = CreateValue(key);
            if (!await map.TryAddAsync(key, value, false)) throw new Exception("Could not arrange the test");
            var ttl = CreateTtl();
            var now = DateTimeOffset.UtcNow;
            var expiration = now.Add(ttl);
            var updatedExpiration = now.Add(ttl + ttl + ttl);
            await map.SetAbsoluteKeyExpirationAsync(key, expiration);

            // Act
            await map.SetAbsoluteKeyExpirationAsync(key, updatedExpiration);

            // Assert
            await Task.Delay(ttl);
            var actual = await map.GetValueOrDefaultAsync(key);
            AssertEquals(actual, value);
            await Task.Delay(ttl + ttl + ttl);
            var contains = await map.ContainsKeyAsync(key);
            AssertIsFalse(contains);
        }

        [Fact(DisplayName = nameof(ExpireInvalidKeyByRelativeTtlReturnsFalse))]
        public async Task ExpireInvalidKeyByRelativeTtlReturnsFalse()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var ttl = CreateTtl();

            // Act
            var actual = await map.SetRelativeKeyExpirationAsync(key, ttl);

            // Assert
            actual.ShouldBeFalse();
        }

        [Fact(DisplayName = nameof(ExpireInvalidKeyByExpirationDateReturnsFalse))]
        public async Task ExpireInvalidKeyByExpirationDateReturnsFalse()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var ttl = CreateTtl();
            var expiration = DateTimeOffset.UtcNow.Add(ttl);

            // Act
            var actual = await map.SetAbsoluteKeyExpirationAsync(key, expiration);

            // Assert
            actual.ShouldBeFalse();
        }

        [Fact(DisplayName = nameof(RemoveExpirationFromExistentVolatileKeyReturnsTrue))]
        public async Task RemoveExpirationFromExistentVolatileKeyReturnsTrue()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var value = CreateValue(key);
            if (!await map.TryAddAsync(key, value, false)) throw new Exception("Could not arrange the test");
            var ttl = CreateTtl();
            var now = DateTimeOffset.UtcNow;
            var expiration = now.Add(ttl);
            await map.SetAbsoluteKeyExpirationAsync(key, expiration);

            //Act
            var removedExpired = await map.RemoveExpirationAsync(key);

            //Assert
            removedExpired.ShouldBeTrue();
        }

        [Fact(DisplayName = nameof(RemoveExpirationFromExistentPersistentKeyReturnsFalse))]
        public async Task RemoveExpirationFromExistentPersistentKeyReturnsFalse()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var value = CreateValue(key);
            if (!await map.TryAddAsync(key, value, false)) throw new Exception("Could not arrange the test");

            //Act
            var removedExpired = await map.RemoveExpirationAsync(key);

            //Assert
            removedExpired.ShouldBeFalse();
        }
    }
}
