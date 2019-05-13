using Take.Elephant.Memory;
using Xunit;

namespace Take.Elephant.Tests.Memory
{
    [Trait("Category", nameof(Memory))]
    public class MemoryItemSenderReceiverQueueFacts : ItemSenderReceiverQueueFacts
    {
        public override (ISenderQueue<Item>, IBlockingReceiverQueue<Item>) Create()
        {
            var queue = new Queue<Item>();
            return (queue, queue);
        }
    }
}