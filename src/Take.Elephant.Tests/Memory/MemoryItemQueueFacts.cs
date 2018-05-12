using Take.Elephant.Memory;
using Xunit;

namespace Take.Elephant.Tests.Memory
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
