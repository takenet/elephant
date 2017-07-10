using System;
using Takenet.Elephant.Memory;
using Xunit;

namespace Takenet.Elephant.Tests.Memory
{
    [Trait("Category", nameof(Memory))]
    public class MemoryGuidNumberMapFacts : GuidNumberMapFacts
    {
        public override INumberMap<Guid> Create()
        {
            return new NumberMap<Guid>();
        }
    }
}
