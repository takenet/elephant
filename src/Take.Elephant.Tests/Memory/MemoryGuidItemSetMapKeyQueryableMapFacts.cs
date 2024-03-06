using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Take.Elephant.Memory;
using Xunit;

namespace Take.Elephant.Tests.Memory
{
    [Trait("Category", nameof(Memory))]
    public class MemoryGuidItemSetMapKeyQueryableMapFacts : GuidItemKeyQueryableMapFacts
    {
        public override async Task<IKeyQueryableMap<Guid, Item>> CreateAsync(params KeyValuePair<Guid, Item>[] values)
        {
            var setMap = new SetMap<Guid, Item>();

            foreach (var keyValuePair in values)
            {
                await setMap.AddItemAsync(keyValuePair.Key, keyValuePair.Value);
            }

            return setMap;
        }
    }
}
