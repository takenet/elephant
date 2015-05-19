using System;
using Takenet.Elephant.Memory;

namespace Takenet.Elephant.Tests.Memory
{
    public class MemoryGuidSetFacts : GuidSetFacts
    {
        public override ISet<Guid> Create()
        {
            return new Set<Guid>();
        }
    }
}
