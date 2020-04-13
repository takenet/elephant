using System;
using Take.Elephant.Memory;
using Xunit;

namespace Take.Elephant.Tests.Specialized
{
    [Trait("Category", nameof(Memory))]
    public class MemoryGuidItemOnDemandCacheMapFacts : GuidItemOnDemandCacheMapFacts
    {
        public override IMap<Guid, Item> CreateSource()
        {
            return new Map<Guid, Item>();
        }

        public override IMap<Guid, Item> CreateCache()
        {
            return new Map<Guid, Item>();
        }
    }
}