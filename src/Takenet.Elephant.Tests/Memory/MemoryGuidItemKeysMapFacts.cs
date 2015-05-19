using System;
using Takenet.Elephant.Memory;

namespace Takenet.Elephant.Tests.Memory
{
    public class MemoryGuidItemKeysMapFacts : GuidItemKeysMapFacts
    {
        public override IKeysMap<Guid, Item> Create()
        {
            return new Map<Guid, Item>();
        }
    }
}
