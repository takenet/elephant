using System;
using System.Linq.Expressions;

namespace Takenet.Elephant.Tests
{
    public abstract class ItemOrderedQueryableStorageFacts : OrderedQueryableStorageFacts<Item, string>
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

        public override Expression<Func<Item, string>> CreateOrderBy()
        {
            return item => item.StringProperty;
        }

        public override Item CreateValue(int order)
        {
            var item = base.CreateValue(order);
            item.StringProperty = $"{order} {item.StringProperty}";
            return item;
        }
    }
}