using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Takenet.SimplePersistence.Memory;

namespace Takenet.SimplePersistence.Tests.Memory
{
    public class MemoryStringSetFacts : StringSetFacts
    {
        public override ISet<string> Create()
        {
            return new HashSetSet<string>();
        }
    }
}
