using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NFluent;
using Ploeh.AutoFixture;
using Xunit;

namespace Takenet.SimplePersistence.Tests
{
    public abstract class ItemSetMapFacts<TKey, TValue> : FactsBase
    {
        public abstract IItemSetMap<TKey, TValue> Create();

        public virtual TKey CreateKey()
        {
            return Fixture.Create<TKey>();
        }

        public virtual TValue CreateValue(TKey key)
        {
            return Fixture.Create<TValue>();
        }

        public virtual ISet<TValue> CreateValueSet(TKey key)
        {
            return new Takenet.SimplePersistence.Memory.Set<TValue>();
        }

        [Fact(DisplayName = "GetExistingItemSucceeds")]
        public async Task GetExistingItemSucceeds()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();            
            var set = CreateValueSet(key);
            var item1 = CreateValue(key);
            var item2 = CreateValue(key);
            var item3 = CreateValue(key);
            await set.AddAsync(item1);
            await set.AddAsync(item2);
            await set.AddAsync(item3);
            await map.TryAddAsync(key, set);

            // Act
            var actual = await map.GetItemOrDefaultAsync(key, item2);

            // Assert
            AssertEquals(actual, item2);
        }

        [Fact(DisplayName = "GetInvalidItemReturnsDefault")]
        public async Task GetInvalidItemReturnsDefault()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var set = CreateValueSet(key);
            var item1 = CreateValue(key);
            var item2 = CreateValue(key);
            var item3 = CreateValue(key);
            await set.AddAsync(item1);            
            await set.AddAsync(item3);
            await map.TryAddAsync(key, set);

            // Act
            var actual = await map.GetItemOrDefaultAsync(key, item2);

            // Assert
            AssertIsDefault(actual);
        }
    }
}
