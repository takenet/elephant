using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Ploeh.AutoFixture;
using Takenet.SimplePersistence.Memory;

namespace Takenet.SimplePersistence.Tests.Memory
{
    public class MemoryGuidItemSetMapFacts : GuidItemSetMapFacts
    {
        public override IMap<Guid, ISet<Item>> Create()
        {
            return new SetMap<Guid, Item>();
        }

        public override ISet<Item> CreateValue(Guid key)
        {
            var set = new SimplePersistence.Memory.Set<Item>();
            set.AddAsync(Fixture.Create<Item>()).Wait();
            set.AddAsync(Fixture.Create<Item>()).Wait();
            set.AddAsync(Fixture.Create<Item>()).Wait();
            return set;
        }
    }
}
