﻿using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Take.Elephant.Tests
{
    public abstract class GuidItemKeyValueMapQueryableStorageFacts : KeyValueMapQueryableStorageFacts<Guid, Item>
    {
        public override Expression<Func<KeyValuePair<Guid, Item>, bool>> CreateFilter(KeyValuePair<Guid, Item> value)
        {
            var randomGuid = Guid.NewGuid();

            return
                i =>
                    i.Key != randomGuid &&
                    i.Key.Equals(value.Key) &&
                    i.Value.GuidProperty == value.Value.GuidProperty &&
                    i.Value.IntegerProperty.Equals(value.Value.IntegerProperty) &&
                    i.Value.IntegerProperty != -11241213 &&
                    i.Value.StringProperty.Equals(value.Value.StringProperty);
        }
    }
}
