using System;

namespace Take.Elephant.Tests
{
    public abstract class GuidItemItemSetMapFacts : ItemSetMapFacts<Guid, Item>
    {
        public override Item CreateValue(Guid key)
        {
            var value = base.CreateValue(key);
            value.RandomProperty = null;
            return value;            
        }
    }
}
