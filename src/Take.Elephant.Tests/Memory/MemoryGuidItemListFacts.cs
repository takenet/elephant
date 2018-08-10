using System;
using Take.Elephant.Memory;

namespace Take.Elephant.Tests.Memory
{
    public class MemoryGuidItemListFacts : GuidItemListFacts
    {
        public override IList<Guid> Create()
        {
            return new List<Guid>();
        }
    }
}