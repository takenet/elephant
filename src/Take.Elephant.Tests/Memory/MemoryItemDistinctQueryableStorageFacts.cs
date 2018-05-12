using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Take.Elephant.Tests.Memory
{
    [Trait("Category", nameof(Memory))]
    public class MemoryItemDistinctQueryableStorageFacts : ItemDistinctQueryableStorageFacts
    {
        public override async Task<IDistinctQueryableStorage<Item>> CreateAsync(params Item[] values)
        {
            var list = new Take.Elephant.Memory.List<Item>();
            foreach (var value in values)
            {
                await list.AddAsync(value);
            }
            return list;
        }
    }
}
