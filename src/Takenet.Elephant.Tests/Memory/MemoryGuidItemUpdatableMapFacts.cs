using System;
using Takenet.Elephant.Memory;
using Xunit;

namespace Takenet.Elephant.Tests.Memory
{
    [Trait("Category", nameof(Memory))]
    public class MemoryGuidItemUpdatableMapFacts : GuidItemUpdatableMapFacts
    {
        public override IUpdatableMap<Guid, Item> Create()
        {
            return new Map<Guid, Item>();
        }
    }
}
