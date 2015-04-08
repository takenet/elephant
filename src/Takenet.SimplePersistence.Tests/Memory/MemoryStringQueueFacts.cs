using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence.Tests.Memory
{
    public class MemoryStringQueueFacts : StringQueueFacts
    {
        public override IQueue<string> Create()
        {
            return new SimplePersistence.Memory.Queue<string>();
        }
    }
}
