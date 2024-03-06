using System;

namespace Take.Elephant.Tests.Memory
{
    public class MemoryGuidItemSortedSetFacts : GuidItemSortedSetFacts
    {
        public override ISortedSet<Guid> Create()
        {
            return new Elephant.Memory.SortedSet<Guid>();
        }
    }
}
