using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Ploeh.AutoFixture;
using Takenet.SimplePersistence.Memory;

namespace Takenet.SimplePersistence.Tests.Memory
{
    public class MemoryGuidItemQueueMapFacts : GuidItemQueueMapFacts
    {
        public override IMap<Guid, IQueue<Item>> Create()
        {
            return new QueueMap<Guid, Item>();
        }

        public override IQueue<Item> CreateValue(Guid key)
        {
            var queue = new SimplePersistence.Memory.Queue<Item>();
            queue.EnqueueAsync(Fixture.Create<Item>()).Wait();
            queue.EnqueueAsync(Fixture.Create<Item>()).Wait();
            queue.EnqueueAsync(Fixture.Create<Item>()).Wait();
            return queue;
        }
    }
}
