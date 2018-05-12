using System;
using Take.Elephant.Memory;
using Xunit;

namespace Take.Elephant.Tests.Memory
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
