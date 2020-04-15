using System;
using AutoFixture;
using Take.Elephant.Memory;
using Take.Elephant.Specialized.Cache;
using Xunit;

namespace Take.Elephant.Tests.Specialized
{
    [Trait("Category", nameof(Memory))]
    public class MemoryMemoryGuidItemDistributedCacheSetMapFacts : GuidItemDistributedCacheSetMapFacts
    {
        public override IMap<Guid, ISet<Item>> CreateSource()
        {
            return new SetMap<Guid, Item>();
        }
        
        public override IMap<Guid, ISet<Item>> CreateCache()
        {
            return new SetMap<Guid, Item>();
        }

        public override IBus<string, SynchronizationEvent<Guid>> CreateSynchronizationBus()
        {
            return new Bus<string, SynchronizationEvent<Guid>>();
        }

        public override ISet<Item> CreateValue(Guid key, bool populate)
        {
            var set = new Set<Item>();
            if (populate)
            {
                set.AddAsync(Fixture.Create<Item>()).Wait();
                set.AddAsync(Fixture.Create<Item>()).Wait();
                set.AddAsync(Fixture.Create<Item>()).Wait();
            }
            return set;
        }
        
        
    }
}