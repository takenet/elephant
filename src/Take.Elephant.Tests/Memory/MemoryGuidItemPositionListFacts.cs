using System;
using Take.Elephant.Memory;
using Xunit;

namespace Take.Elephant.Tests.Memory
{
    [Trait("Category", nameof(Memory))]
    public class MemoryGuidItemPositionListFacts : GuidItemPositionListFacts
    {
        public override IPositionList<Guid> Create()
        {
            return new List<Guid>();
        }
    }
}