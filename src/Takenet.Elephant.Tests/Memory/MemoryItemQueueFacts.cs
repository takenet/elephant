using Takenet.Elephant.Memory;
using Xunit;

namespace Takenet.Elephant.Tests.Memory
{
    [Trait("Category", nameof(Memory))]
    public class MemoryItemQueueFacts : ItemQueueFacts
    {
        public override IQueue<Item> Create()
        {
            return new Queue<Item>();
        }
    }
}
