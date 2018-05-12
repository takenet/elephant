using System;
using Take.Elephant.Memory;
using Xunit;

namespace Take.Elephant.Tests.Memory
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
