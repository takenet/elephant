using System;
using System.Linq.Expressions;

namespace Takenet.Elephant.Tests
{
    public abstract class ItemDistinctQueryableStorageFacts : DistinctQueryableStorageFacts<Item>
    {
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