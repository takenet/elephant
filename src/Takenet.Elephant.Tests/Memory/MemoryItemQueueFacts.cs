using Takenet.Elephant.Memory;

namespace Takenet.Elephant.Tests.Memory
{
    public class MemoryItemQueueFacts : ItemQueueFacts
    {
        public override IQueue<Item> Create()
        {
            return new Queue<Item>();
        }
    }
}
