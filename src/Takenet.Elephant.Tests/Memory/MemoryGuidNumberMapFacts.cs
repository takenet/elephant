using System;
using Takenet.Elephant.Memory;

namespace Takenet.Elephant.Tests.Memory
{
    public class MemoryGuidNumberMapFacts : GuidNumberMapFacts
    {
        public override INumberMap<Guid> Create()
        {
            return new NumberMap<Guid>();
        }
    }
}
