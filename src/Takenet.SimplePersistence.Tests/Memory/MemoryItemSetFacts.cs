using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Takenet.SimplePersistence.Memory;

namespace Takenet.SimplePersistence.Tests.Memory
{
    public class MemoryItemSetFacts : ItemSetFacts
    {
        public override ISet<Item> Create()
        {
            return new SimplePersistence.Memory.HashSet<Item>();
        }

    }
}
