using System;
using Take.Elephant.Memory;
using Xunit;

namespace Take.Elephant.Tests.Memory
{
    [Trait("Category", nameof(Memory))]
    public class MemoryGuidItemItemSetMapFacts : GuidItemItemSetMapFacts
    {
        public override IItemSetMap<Guid, Item> Create()
        {
            return new SetMap<Guid, Item>();
        }
    }
}
