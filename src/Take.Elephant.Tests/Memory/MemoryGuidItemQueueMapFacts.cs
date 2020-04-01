using System;
using AutoFixture;
using Take.Elephant.Memory;
using Xunit;

namespace Take.Elephant.Tests.Memory
{
    [Trait("Category", nameof(Memory))]
    public class MemoryGuidItemQueueMapFacts : GuidItemQueueMapFacts
    {
        public override IMap<Guid, IQueue<Item>> Create()
        {
            return new QueueMap<Guid, Item>();
        }

        public override IQueue<Item> CreateValue(Guid key)
        {
            var queue = new Elephant.Memory.Queue<Item>();
            queue.EnqueueAsync(Fixture.Create<Item>()).Wait();
            queue.EnqueueAsync(Fixture.Create<Item>()).Wait();
            queue.EnqueueAsync(Fixture.Create<Item>()).Wait();
            return queue;
        }
    }
}
