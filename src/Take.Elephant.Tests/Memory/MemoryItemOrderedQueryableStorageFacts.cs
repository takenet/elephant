﻿using System.Threading.Tasks;
using Take.Elephant.Memory;
using Xunit;

namespace Take.Elephant.Tests.Memory
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
