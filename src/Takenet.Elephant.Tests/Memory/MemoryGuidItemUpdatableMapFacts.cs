using System;
using Takenet.Elephant.Memory;

namespace Takenet.Elephant.Tests.Memory
{
    public class MemoryGuidItemUpdatableMapFacts : GuidItemUpdatableMapFacts
    {
        public override IUpdatableMap<Guid, Item> Create()
        {
            return new Map<Guid, Item>();
        }
    }
}
