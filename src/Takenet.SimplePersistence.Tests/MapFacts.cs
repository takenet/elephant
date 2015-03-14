using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NFluent;
using Ploeh.AutoFixture.Xunit;
using Xunit;
using Xunit.Extensions;

namespace Takenet.SimplePersistence.Tests
{
    public abstract class MapFacts<TKey, TValue>
    {
        public abstract IMap<TKey, TValue> Create();

        [Fact]
        public void FakeFact()
        {
            
        }

            
        [Theory, AutoData]
        public async Task AddNewKeyAndValueShouldSucceed(TKey key, TValue value)
        {
            // Arrange
            var map = Create();

            // Act
            var result = await map.TryAddAsync(key, value, false);

            // Assert
            Check.That(result).IsTrue();
            Check.That(await map.GetValueOrDefaultAsync(key)).IsEqualTo(value);
        }

    }
}
