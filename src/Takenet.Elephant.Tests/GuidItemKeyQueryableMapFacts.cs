using System;
using System.Linq.Expressions;

namespace Takenet.Elephant.Tests
{
    public abstract class GuidItemKeyQueryableMapFacts : KeyQueryableMapFacts<Guid, Item>
    {
        public override Expression<Func<Item, bool>> CreateFilter(Item value)
        {
            return
                i =>
                    i.GuidProperty.Equals(value.GuidProperty) && 
                    i.IntegerProperty.Equals(value.IntegerProperty) &&
                    i.StringProperty.Equals(value.StringProperty);
        }
    }
}