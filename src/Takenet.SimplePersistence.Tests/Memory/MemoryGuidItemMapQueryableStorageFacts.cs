using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Takenet.SimplePersistence.Memory;

namespace Takenet.SimplePersistence.Tests.Memory
{
    public class MemoryGuidItemMapQueryableStorageFacts : GuidItemMapQueryableStorageFacts
    {
        public override IMap<Guid, Item> Create()
        {
            return new Map<Guid, Item>();
        }
    }
}
