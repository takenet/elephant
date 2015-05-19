using System;
using Takenet.Elephant.Memory;

namespace Takenet.Elephant.Tests.Memory
{
    public class MemoryGuidItemItemSetMapFacts : GuidItemItemSetMapFacts
    {
        public override IItemSetMap<Guid, Item> Create()
        {
            return new SetMap<Guid, Item>();
        }
    }
}
