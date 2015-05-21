using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Takenet.Elephant.Memory;

namespace Takenet.Elephant.Tests.Memory
{
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
