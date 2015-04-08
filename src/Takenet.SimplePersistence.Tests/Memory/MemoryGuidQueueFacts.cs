using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence.Tests.Memory
{
    public class MemoryGuidQueueFacts : GuidQueueFacts
    {
        public override IQueue<Guid> Create()
        {
            return new SimplePersistence.Memory.Queue<Guid>();
        }
    }
}