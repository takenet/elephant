using System;
using Take.Elephant.Memory;
using Xunit;

namespace Take.Elephant.Tests.Memory
{
    [Trait("Category", nameof(Memory))]
    public class MemoryGuidItemMapFacts : GuidItemMapFacts
    {
        public override IMap<Guid, Item> Create()
        {
            return new Map<Guid, Item>();            
        }
    }
}
