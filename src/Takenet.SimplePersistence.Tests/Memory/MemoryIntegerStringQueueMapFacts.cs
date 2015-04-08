using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Ploeh.AutoFixture;
using Takenet.SimplePersistence.Memory;

namespace Takenet.SimplePersistence.Tests.Memory
{
    public class MemoryIntegerStringQueueMapFacts : IntegerStringQueueMapFacts
    {
        public override IMap<int, IQueue<string>> Create()
        {
            return new QueueMap<int, string>();
        }

        public override IQueue<string> CreateValue(int key)
        {
            var queue = new SimplePersistence.Memory.Queue<string>();
            queue.EnqueueAsync(Fixture.Create<string>()).Wait();
            queue.EnqueueAsync(Fixture.Create<string>()).Wait();
            queue.EnqueueAsync(Fixture.Create<string>()).Wait();
            return queue;
        }
    }
}
