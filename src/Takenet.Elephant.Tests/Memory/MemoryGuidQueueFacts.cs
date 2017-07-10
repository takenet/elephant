using System;
using Xunit;

namespace Takenet.Elephant.Tests.Memory
{
    [Trait("Category", nameof(Memory))]
    public class MemoryGuidQueueFacts : GuidQueueFacts
    {
        public override IQueue<Guid> Create()
        {
            return new Elephant.Memory.Queue<Guid>();
        }
    }
}