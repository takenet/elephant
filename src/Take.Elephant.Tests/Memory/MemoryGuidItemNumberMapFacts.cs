using System;
using Take.Elephant.Memory;
using Xunit;

namespace Take.Elephant.Tests.Memory
{
    [Trait("Category", nameof(Memory))]
    public class MemoryGuidItemNumberMapFacts : GuidItemNumberMapFacts
    {
        public override INumberMap<Guid> Create()
        {
            return new NumberMap<Guid>();
        }
    }
}
