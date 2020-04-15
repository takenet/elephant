using System;
using Take.Elephant.Memory;
using Take.Elephant.Specialized.Cache;
using Xunit;

namespace Take.Elephant.Tests.Specialized
{
    [Trait("Category", nameof(Memory))]
    public class MemoryMemoryGuidItemDistributedCacheMapFacts : GuidItemDistributedCacheMapFacts
    {
        public override IMap<Guid, Item> CreateSource()
        {
            return new Map<Guid, Item>();
        }

        public override IMap<Guid, Item> CreateCache()
        {
            return new Map<Guid, Item>();
        }

        public override IBus<string, SynchronizationEvent<Guid>> CreateSynchronizationBus()
        {
            return new Bus<string, SynchronizationEvent<Guid>>();
        }
    }
}