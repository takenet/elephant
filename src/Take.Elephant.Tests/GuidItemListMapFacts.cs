using System;
using System.Threading.Tasks;
using Xunit;

namespace Take.Elephant.Tests
{
    public abstract class GuidItemListMapFacts : ListMapFacts<Guid, Item>
    {
        [Fact(DisplayName = "GetEmptyListSucceds")]
        public virtual async Task GetEmptyListSucceds()
        {
            // Arrange
            var map = (IListMap<Guid, Item>)Create();
            var key = CreateKey();
            var value = CreateValue(key, false);
            var emptyList = await map.GetValueOrEmptyAsync(key);

            // Assert
            AssertEquals(await emptyList.GetLengthAsync(), await value.GetLengthAsync());
        }
    }
}