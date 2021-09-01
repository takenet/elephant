using Take.Elephant.Memory;
using Xunit;

namespace Take.Elephant.Tests.Memory
{
    [Trait("Category", nameof(Memory))]
    public class MemoryItemSenderReceiverQueueFacts : ItemSenderReceiverQueueFacts
    {
        public override (IStreamSenderQueue<Item>, IBlockingReceiverQueue<Item>) Create()
        {
            var queue = new StreamQueue<Item>();
            return (queue, queue);
        }
    }
}