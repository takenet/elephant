using System;
using Takenet.Elephant.Memory;
using Xunit;

namespace Takenet.Elephant.Tests.Memory
{
    [Trait("Category", nameof(Memory))]
    public class MemoryGuidSetFacts : GuidSetFacts
    {
        public override ISet<Guid> Create()
        {
            return new Set<Guid>();
        }
    }
}
