using System;
using Takenet.Elephant.Memory;

namespace Takenet.Elephant.Tests.Memory
{
    public class MemoryGuidItemPropertyMapFacts : GuidItemPropertyMapFacts
    {
        public override IPropertyMap<Guid, Item> Create()
        {
            return new Map<Guid, Item>();
        }
    }
}
