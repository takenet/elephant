using System;
using Takenet.Elephant.Memory;

namespace Takenet.Elephant.Tests.Memory
{
    public class MemoryGuidItemExpirableKeyMapFacts : GuidItemExpirableKeyMapFacts
    {
        public override IExpirableKeyMap<Guid, Item> Create()
        {
            return new Map<Guid, Item>();
        }
    }
}
