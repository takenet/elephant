using System;
using Take.Elephant.Memory;
using Take.Elephant.Specialized.Cache;

namespace Take.Elephant.Tests.Specialized
{
    public class MemoryGuidItemDistributedCacheMapFacts : GuidItemDistributedCacheMapFacts
    {
        public override IMap<Guid, Item> CreateSource()
        {
            return new Map<Guid, Item>();
        }

        public override IBus<string, SynchronizationEvent<Guid>> CreateSynchronizationBus()
        {
            return new Bus<string, SynchronizationEvent<Guid>>();
        }
    }
}