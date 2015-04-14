using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Takenet.SimplePersistence.Memory;

namespace Takenet.SimplePersistence.Tests.Memory
{
    public class MemoryGuidItemKeysMapFacts : GuidItemKeysMapFacts
    {
        public override IKeysMap<Guid, Item> Create()
        {
            return new Map<Guid, Item>();
        }
    }
}
