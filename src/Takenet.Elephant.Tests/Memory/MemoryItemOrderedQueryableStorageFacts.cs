using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Takenet.Elephant.Memory;
using Xunit;

namespace Takenet.Elephant.Tests.Memory
{
    [Trait("Category", nameof(Memory))]
    public class MemoryItemOrderedQueryableStorageFacts : ItemOrderedQueryableStorageFacts
    {
        public override Task<IOrderedQueryableStorage<Item>> CreateAsync(params Item[] values)
        {
            var set = new Set<Item>(values);
            return set.AsCompletedTask<IOrderedQueryableStorage<Item>>();
        }

    }
}
