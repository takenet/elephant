using System;
using Takenet.Elephant.Memory;
using Xunit;

namespace Takenet.Elephant.Tests.Memory
{
    [Trait("Category", nameof(Memory))]
    public class MemoryGuidItemKeysMapFacts : GuidItemKeysMapFacts
    {
        public override IKeysMap<Guid, Item> Create()
        {
            return new Map<Guid, Item>();
        }
    }
}
