using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Takenet.SimplePersistence.Memory;

namespace Takenet.SimplePersistence.Tests.Memory
{
    public class MemoryGuidItemItemSetMapFacts : GuidItemItemSetMapFacts
    {
        public override IItemSetMap<Guid, Item> Create()
        {
            return new SetMap<Guid, Item>();
        }
    }
}
