using Take.Elephant.Memory;
using Xunit;

namespace Take.Elephant.Tests.Memory
{
    [Trait("Category", nameof(Memory))]
    public class MemoryItemBatchReceiverQueueFacts : ItemBatchReceiverQueueFacts
    {
        public override IBatchReceiverQueue<Item> Create(params Item[] items)
        {
            var queue = new Queue<Item>();
            queue.EnqueueBatchAsync(items).Wait();
            return queue;
        }
    }
}
