using System;
using Ploeh.AutoFixture;
using Take.Elephant.Memory;

namespace Take.Elephant.Tests.Memory
{
    public class MemoryGuidItemListMapFacts : GuidItemListMapFacts
    {
        public override IMap<Guid, IList<Item>> Create()
        {
            return new ListMap<Guid, Item>();
        }

        public override IList<Item> CreateValue(Guid key, bool populate)
        {
            var list = new List<Item>();
            if (populate)
            {
                list.AddAsync(Fixture.Create<Item>()).Wait();
                list.AddAsync(Fixture.Create<Item>()).Wait();
                list.AddAsync(Fixture.Create<Item>()).Wait();
            }
            return list;
        }
    }
}
