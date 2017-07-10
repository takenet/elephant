using System;
using Takenet.Elephant.Memory;
using Xunit;

namespace Takenet.Elephant.Tests.Memory
{
    [Trait("Category", nameof(Memory))]
    public class MemoryGuidItemExpirableKeyMapFacts : GuidItemExpirableKeyMapFacts
    {
        public override IExpirableKeyMap<Guid, Item> Create()
        {
            return new Map<Guid, Item>();
        }
    }
}
