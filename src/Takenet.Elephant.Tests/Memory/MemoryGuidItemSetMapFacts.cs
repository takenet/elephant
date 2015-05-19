using System;
using Ploeh.AutoFixture;
using Takenet.Elephant.Memory;

namespace Takenet.Elephant.Tests.Memory
{
    public class MemoryGuidItemSetMapFacts : GuidItemSetMapFacts
    {
        public override IMap<Guid, ISet<Item>> Create()
        {
            return new SetMap<Guid, Item>();
        }

        public override ISet<Item> CreateValue(Guid key)
        {
            var set = new Set<Item>();
            set.AddAsync(Fixture.Create<Item>()).Wait();
            set.AddAsync(Fixture.Create<Item>()).Wait();
            set.AddAsync(Fixture.Create<Item>()).Wait();
            return set;
        }
    }
}
