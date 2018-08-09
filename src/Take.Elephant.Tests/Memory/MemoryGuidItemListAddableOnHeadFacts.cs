using System;
using Take.Elephant.Memory;
using Xunit;

namespace Take.Elephant.Tests.Memory
{
    [Trait("Category", nameof(Memory))]
    public class MemoryGuidItemListAddableOnHeadFacts : GuidItemListAddableOnHeadFacts
    {
        public override IListAddableOnHead<Guid> Create()
        {
            return new List<Guid>();
        }
    }
}