using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Takenet.Elephant.Memory;

namespace Takenet.Elephant.Tests.Memory
{
    public class MemoryGuidItemKeyValueSetMapQueryableStorageFacts : QueryableStorageFacts<KeyValuePair<Guid, Item>>
    {
        public override async Task<IQueryableStorage<KeyValuePair<Guid, Item>>> CreateAsync(params KeyValuePair<Guid, Item>[] values)
        {
            var setMap = new SetMap<Guid, Item>();

            foreach (var keyValuePair in values)
            {
                await setMap.AddItemAsync(keyValuePair.Key, keyValuePair.Value);
            }

            return setMap;
        }

        public override Expression<Func<KeyValuePair<Guid, Item>, bool>> CreateFilter(KeyValuePair<Guid, Item> value)
        {
            var randomGuid = Guid.NewGuid();

            return
                i =>
                    i.Key != randomGuid &&
                    i.Key.Equals(value.Key) &&
                    i.Value.GuidProperty == value.Value.GuidProperty &&
                    i.Value.IntegerProperty.Equals(value.Value.IntegerProperty) &&
                    i.Value.IntegerProperty != -11241213 &&
                    i.Value.StringProperty.Equals(value.Value.StringProperty);
        }
    }
}
