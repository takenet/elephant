using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Takenet.SimplePersistence.Memory;

namespace Takenet.SimplePersistence.Tests.Memory
{
    public class MemoryGuidNumberMapFacts : GuidNumberMapFacts
    {
        public override INumberMap<Guid> Create()
        {
            return new NumberMap<Guid>();
        }
    }
}
