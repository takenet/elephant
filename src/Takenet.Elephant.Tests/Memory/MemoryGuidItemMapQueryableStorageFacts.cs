using System;
using Takenet.Elephant.Memory;

namespace Takenet.Elephant.Tests.Memory
{
    public class MemoryGuidItemMapQueryableStorageFacts : GuidItemMapQueryableStorageFacts
    {
        public override IMap<Guid, Item> Create()
        {
            return new Map<Guid, Item>();
        }
    }
}
