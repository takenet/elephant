using System;
using Take.Elephant.Memory;
using Xunit;

namespace Take.Elephant.Tests.Memory
{
    [Trait("Category", nameof(Memory))]
    public class MemoryGuidItemPropertyMapFacts : GuidItemPropertyMapFacts
    {
        public override IPropertyMap<Guid, Item> Create()
        {
            return new Map<Guid, Item>();
        }
    }
}
