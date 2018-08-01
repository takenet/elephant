using System;
using Ploeh.AutoFixture;
using Take.Elephant.Memory;

namespace Take.Elephant.Tests.Memory
{
    public class MemoryGuidItemListMapFacts : GuidItemListMapFacts
    {
        public override IMap<Guid, IList<Item>> Create()
        {
            return new ListMap<Guid, Item>();
        }
    }
}
