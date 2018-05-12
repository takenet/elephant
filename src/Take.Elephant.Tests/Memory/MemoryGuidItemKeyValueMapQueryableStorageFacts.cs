using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Take.Elephant.Memory;
using Xunit;

namespace Take.Elephant.Tests.Memory
{
    [Trait("Category", nameof(Memory))]
    public class MemoryGuidItemKeyValueMapQueryableStorageFacts : GuidItemKeyValueMapQueryableStorageFacts
    {
        public override IMap<Guid, Item> Create()
        {
            return new Map<Guid, Item>();
        }
    }
}
