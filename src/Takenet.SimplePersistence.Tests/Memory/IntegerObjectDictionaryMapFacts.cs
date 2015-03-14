using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NFluent;
using Ploeh.AutoFixture.Xunit;
using Takenet.SimplePersistence.Memory;
using Xunit;
using Xunit.Extensions;

namespace Takenet.SimplePersistence.Tests.Memory
{
    public class IntegerObjectDictionaryMapFacts 
    {
        public IMap<int, object> Create()
        {
            return new DictionaryMap<int, object>();            
        }



        [Fact]
        public void Fact()
        { }

        [Theory, AutoData]
        public async Task AddNewKeyAndValueShouldSucceed(int key, object value)
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
