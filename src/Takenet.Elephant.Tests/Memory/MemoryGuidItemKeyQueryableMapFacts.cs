using System;
using Takenet.Elephant.Memory;

namespace Takenet.Elephant.Tests.Memory
{
    public class MemoryGuidItemKeyQueryableMapFacts : GuidItemKeyQueryableMapFacts
    {
        public override IKeyQueryableMap<Guid, Item> Create()
        {
            return new Map<Guid, Item>();
        }
    }
}
