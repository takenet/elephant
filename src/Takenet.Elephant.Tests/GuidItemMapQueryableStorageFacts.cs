using System;
using System.Linq.Expressions;

namespace Takenet.Elephant.Tests
{
    public abstract class GuidItemMapQueryableStorageFacts : MapQueryableStorageFacts<Guid, Item>
    {
        public override Expression<Func<Item, bool>> CreateFilter(Item value)
        {
            var randomGuid = Guid.NewGuid();
            var randomString1 = Guid.NewGuid().ToString();
            var randomString2 = Guid.NewGuid().ToString();
            return
                i =>
                    i.GuidProperty != randomGuid &&
                    i.GuidProperty.Equals(value.GuidProperty) &&
                    i.IntegerProperty.Equals(value.IntegerProperty) &&
                    i.IntegerProperty != -1291387 &&
                    !i.StringProperty.StartsWith(randomString1) &&
                    !i.StringProperty.EndsWith(randomString2) &&
                    i.StringProperty.Equals(value.StringProperty) &&
                    i.StringProperty.StartsWith(value.StringProperty) &&
                    i.StringProperty.EndsWith(value.StringProperty) &&
                    i.StringProperty != "ignore me";


        }
    }
}