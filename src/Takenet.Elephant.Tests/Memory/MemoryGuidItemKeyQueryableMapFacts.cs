using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Takenet.Elephant.Memory;
using Xunit;

namespace Takenet.Elephant.Tests.Memory
{
    [Trait("Category", nameof(Memory))]
    public class MemoryGuidItemKeyQueryableMapFacts : GuidItemKeyQueryableMapFacts
    {
        public override async Task<IKeyQueryableMap<Guid, Item>> CreateAsync(params KeyValuePair<Guid, Item>[] values)
        {
            var map = new Map<Guid, Item>();

            foreach (var value in values)
            {
                await map.TryAddAsync(value.Key, value.Value);
            }

            return map;
        }
    }
}
