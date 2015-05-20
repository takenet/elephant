using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.Elephant.Tests
{
    public abstract class GuidItemKeyValueMapQueryableStorageFacts : KeyValueMapQueryableStorageFacts<Guid, Item>
    {
        public override Expression<Func<KeyValuePair<Guid, Item>, bool>> CreateFilter(KeyValuePair<Guid, Item> value)
        {
            return
                i =>
                    i.Key.Equals(value.Key) &&
                    i.Value.GuidProperty.Equals(value.Value.GuidProperty) &&
                    i.Value.IntegerProperty.Equals(value.Value.IntegerProperty) &&
                    i.Value.StringProperty.Equals(value.Value.StringProperty);
        }
    }
}
