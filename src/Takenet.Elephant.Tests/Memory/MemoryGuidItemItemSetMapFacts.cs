using System;
using Takenet.Elephant.Memory;
using Xunit;

namespace Takenet.Elephant.Tests.Memory
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
