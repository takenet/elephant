using Takenet.Elephant.Memory;

namespace Takenet.Elephant.Tests.Memory
{
    public class MemoryItemBlockingQueueFacts : ItemBlockingQueueFacts
    {
        public override IBlockingQueue<Item> Create()
        {
            return new Queue<Item>();
        }
    }
}
