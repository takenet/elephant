using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Memory;
using Xunit;

namespace Take.Elephant.Tests.Memory
{
    [Trait("Category", nameof(Memory))]
    public class MemoryItemSenderReceiverQueueFacts : ItemSenderReceiverQueueFacts
    {
        public override ValueTask<(ISenderQueue<Item>, IBlockingReceiverQueue<Item>)> CreateAsync(CancellationToken cancellationToken)
        {
            var queue = new Queue<Item>();
            return new ValueTask<(ISenderQueue<Item>, IBlockingReceiverQueue<Item>)>((queue, queue));
        }
    }
}