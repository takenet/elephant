using System;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Ploeh.AutoFixture;
using Takenet.Elephant.Memory;
using Xunit;

namespace Takenet.Elephant.Tests.Memory
{
    [Trait("Category", nameof(Memory))]
    public class MemoryGuidItemSetMapQueryableStorageFacts : QueryableStorageFacts<Item>
    {
        public override async Task<IQueryableStorage<Item>> CreateAsync(params Item[] values)
        {
            var setMap = new SetMap<Guid, Item>();

            foreach (var value in values)
            {
                await setMap.AddItemAsync(Fixture.Create<Guid>(), value);
            }

            return setMap;
        }

        public override Expression<Func<Item, bool>> CreateFilter(Item value)
        {
            var randomGuid = Guid.NewGuid();
            return
                i =>
                    i.GuidProperty != randomGuid &&
                    i.GuidProperty.Equals(value.GuidProperty) &&
                    i.IntegerProperty.Equals(value.IntegerProperty) &&
                    i.IntegerProperty != -1291387 &&
                    i.StringProperty.Equals(value.StringProperty);
        }
    }
}
