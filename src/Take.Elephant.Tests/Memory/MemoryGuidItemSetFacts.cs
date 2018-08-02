using System;
using Take.Elephant.Memory;
using Xunit;

namespace Take.Elephant.Tests.Memory
{
    [Trait("Category", nameof(Memory))]
    public class MemoryGuidItemSetFacts : GuidItemSetFacts
    {
        public override ISet<Guid> Create()
        {
            return new Set<Guid>();
        }
    }
}
