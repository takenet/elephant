using System;
using Xunit;

namespace Take.Elephant.Tests.Memory
{
    [Trait("Category", nameof(Memory))]
    public class MemoryGuidItemQueueFacts : GuidItemQueueFacts
    {
        public override IQueue<Guid> Create()
        {
            return new Elephant.Memory.Queue<Guid>();
        }
    }
}