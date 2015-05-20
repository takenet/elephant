using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Takenet.Elephant.Memory;

namespace Takenet.Elephant.Tests.Memory
{
    public class MemoryGuidItemKeyValueMapQueryableStorageFacts : GuidItemKeyValueMapQueryableStorageFacts
    {
        public override IMap<Guid, Item> Create()
        {
            return new Map<Guid, Item>();
        }
    }
}
