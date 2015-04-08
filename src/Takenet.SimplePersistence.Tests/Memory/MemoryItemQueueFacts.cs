using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence.Tests.Memory
{
    public class MemoryItemQueueFacts : ItemQueueFacts
    {
        public override IQueue<Item> Create()
        {
            return new SimplePersistence.Memory.Queue<Item>();
        }
    }
}
