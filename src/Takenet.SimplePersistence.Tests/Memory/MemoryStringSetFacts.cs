using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Takenet.SimplePersistence.Memory;

namespace Takenet.SimplePersistence.Tests.Memory
{
    public class MemoryGuidSetFacts : GuidSetFacts
    {
        public override ISet<Guid> Create()
        {
            return new Set<Guid>();
        }
    }
}
