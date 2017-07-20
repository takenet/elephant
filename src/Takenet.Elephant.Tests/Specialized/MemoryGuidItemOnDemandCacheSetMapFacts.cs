using System;
using Ploeh.AutoFixture;
using Takenet.Elephant.Memory;
using Xunit;

namespace Takenet.Elephant.Tests.Specialized
{
    [Trait("Category", nameof(Memory))]
    public class MemoryGuidItemOnDemandCacheSetMapFacts : GuidItemOnDemandCacheSetMapFacts
    {
        public override IMap<Guid, ISet<Item>> CreateSource() => new SetMap<Guid, Item>();

        public override IMap<Guid, ISet<Item>> CreateCache() => new SetMap<Guid, Item>();
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