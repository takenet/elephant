using System;
using System.Threading.Tasks;
using Xunit;

namespace Take.Elephant.Tests
{
    public abstract class GuidItemSetMapFacts : SetMapFacts<Guid, Item>
    {
        [Fact(DisplayName = "GetEmptySetSucceds")]
        public virtual async Task GetEmptyListSucceds()
        {
            // Arrange
            var map = (ISetMap<Guid, Item>)Create();
            var key = CreateKey();
            var value = CreateValue(key, false);
            var emptySet = await map.GetValueOrEmptyAsync(key);

            // Assert
            AssertEquals(await emptySet.GetLengthAsync(), await value.GetLengthAsync());
        }
    }
}