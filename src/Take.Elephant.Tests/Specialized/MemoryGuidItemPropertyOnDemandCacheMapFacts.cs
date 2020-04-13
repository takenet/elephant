using System;
using Take.Elephant.Memory;
using Xunit;

namespace Take.Elephant.Tests.Specialized
{
    [Trait("Category", nameof(Memory))]
    public class MemoryGuidItemPropertyOnDemandCacheMapFacts : GuidItemPropertyOnDemandCacheMapFacts
    {
        public override IPropertyMap<Guid, Item> CreateSource()
        {
            return new Map<Guid, Item>();
        }

        public override IPropertyMap<Guid, Item> CreateCache()
        {
            return new Map<Guid, Item>();
        }
    }
}