using System;
using System.Threading.Tasks;
using NFluent;
using Ploeh.AutoFixture;
using Xunit;

namespace Takenet.Elephant.Tests
{
    public abstract class KeysMapFacts<TKey, TValue> : FactsBase
    {
        public abstract IKeysMap<TKey, TValue> Create();

        public virtual TKey CreateKey()
        {
            return Fixture.Create<TKey>();
        }

        public virtual TValue CreateValue(TKey key)
        {
            return Fixture.Create<TValue>();
        }

        [Fact(DisplayName = "GetExistingKeysSucceeds")]
        public virtual async Task GetExistingKeysSucceeds()
        {
            // Arrange
            var map = Create();
            var key1 = CreateKey();
            var key2 = CreateKey();
            var key3 = CreateKey();
            if (!await map.TryAddAsync(key1, CreateValue(key1), false)) throw new Exception("Could not arrange the test");
            if (!await map.TryAddAsync(key2, CreateValue(key2), false)) throw new Exception("Could not arrange the test");
            if (!await map.TryAddAsync(key3, CreateValue(key3), false)) throw new Exception("Could not arrange the test");

            // Act
            var actual = await map.GetKeysAsync();

            // Assert
            var actualList = await actual.ToListAsync();
            Check.That(actualList).Contains(key1);
            Check.That(actualList).Contains(key2);
            Check.That(actualList).Contains(key3);
        }

        [Fact(DisplayName = "GetNonExistingKeysReturnsEmpty")]
        public virtual async Task GetNonExistingKeysReturnsEmpty()
        {
            // Arrange
            var map = Create();

            // Act
            var actual = await map.GetKeysAsync();

            // Assert
            var actualList = await actual.ToListAsync();
            Check.That(actualList).IsEmpty();
        }
    }
}
