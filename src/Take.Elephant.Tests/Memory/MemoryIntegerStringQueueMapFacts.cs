using Ploeh.AutoFixture;
using Take.Elephant.Memory;
using Xunit;

namespace Take.Elephant.Tests.Memory
{
    [Trait("Category", nameof(Memory))]
    public class MemoryIntegerStringQueueMapFacts : IntegerStringQueueMapFacts
    {
        public override IMap<int, IQueue<string>> Create()
        {
            return new QueueMap<int, string>();
        }

        public override IQueue<string> CreateValue(int key)
        {
            var queue = new Elephant.Memory.Queue<string>();
            queue.EnqueueAsync(Fixture.Create<string>()).Wait();
            queue.EnqueueAsync(Fixture.Create<string>()).Wait();
            queue.EnqueueAsync(Fixture.Create<string>()).Wait();
            return queue;
        }
    }
}
