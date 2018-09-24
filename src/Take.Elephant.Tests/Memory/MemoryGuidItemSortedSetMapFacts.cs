using Ploeh.AutoFixture;
using System;
using Take.Elephant.Memory;

namespace Take.Elephant.Tests.Memory
{
    public class MemoryGuidItemSortedSetMapFacts : GuidItemSortedSetMapFacts
    {
        public override IMap<Guid, ISortedSet<Item>> Create()
        {
            return new SortedSetMap<Guid, Item>();
        }

        public override ISortedSet<Item> CreateValue(Guid key, bool populate)
        {
            var list = new SortedSet<Item>();
            if (populate)
            {
                list.AddAsync(Fixture.Create<Item>(), Fixture.Create<double>()).Wait();
                list.AddAsync(Fixture.Create<Item>(), Fixture.Create<double>()).Wait();
                list.AddAsync(Fixture.Create<Item>(), Fixture.Create<double>()).Wait();
            }
            return list;
        }
    }
}