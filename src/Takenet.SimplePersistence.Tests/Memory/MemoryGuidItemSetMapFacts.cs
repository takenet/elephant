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
            Fixture.Register<ISet<Item>>(() => new SimplePersistence.Memory.HashSetSet<Item>());
            return new DictionaryHashSetMap<Guid, Item>();
        }
    }
}
