using System;

namespace Takenet.Elephant.Tests.Memory
{
    public class MemoryGuidQueueFacts : GuidQueueFacts
    {
        public override IQueue<Guid> Create()
        {
            return new Elephant.Memory.Queue<Guid>();
        }
    }
}