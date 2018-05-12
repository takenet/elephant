using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NFluent;
using Ploeh.AutoFixture;
using Xunit;

namespace Take.Elephant.Tests
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
            var random = new Random();
            var count = random.Next(10, 100);
            var keys = new List<TKey>();
            for (var i = 0; i < count; i++)
            {
                var key = CreateKey();
                keys.Add(key);
                if (!await map.TryAddAsync(key, CreateValue(key), false)) throw new Exception("Could not arrange the test");
            }

            // Act
            var actual = await map.GetKeysAsync();

            // Assert
            var actualList = await actual.ToListAsync();
            foreach (var key in keys)
            {
                Check.That(actualList).Contains(key);
            }            
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
