using System;
using System.Threading.Tasks;
using Ploeh.AutoFixture;
using Xunit;

namespace Takenet.Elephant.Tests
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
            return TimeSpan.FromMilliseconds(250);
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
            var actual = await map.GetValueOrDefaultAsync(key);
            AssertIsDefault(actual);
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
            await Task.Delay(ttl + ttl);

            // Assert
            var actual = await map.GetValueOrDefaultAsync(key);
            AssertIsDefault(actual);
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
            await map.SetRelativeKeyExpirationAsync(key, ttl + ttl + ttl);
            
            // Assert
            await Task.Delay(ttl);
            var actual = await map.GetValueOrDefaultAsync(key);
            AssertEquals(actual, value);
            await Task.Delay(ttl + ttl + ttl);
            actual = await map.GetValueOrDefaultAsync(key);
            AssertIsDefault(actual);
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
            actual = await map.GetValueOrDefaultAsync(key);
            AssertIsDefault(actual);
        }

        [Fact(DisplayName = "ExpireInvalidKeyByRelativeTtlThrowsArgumentException")]
        public async Task ExpireInvalidKeyByRelativeTtlThrowsArgumentException()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var ttl = CreateTtl();

            // Act
            await AssertThrowsAsync<ArgumentException>(() =>
                map.SetRelativeKeyExpirationAsync(key, ttl));
        }

        [Fact(DisplayName = "ExpireInvalidKeyByAbsoluteExpirationDateThrowsArgumentException")]
        public async Task ExpireInvalidKeyByExpirationDateThrowsArgumentException()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var ttl = CreateTtl();
            var expiration = DateTimeOffset.UtcNow.Add(ttl);

            // Act
            await AssertThrowsAsync<ArgumentException>(() =>
                map.SetAbsoluteKeyExpirationAsync(key, expiration));
        }
    }
}
