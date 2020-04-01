using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using AutoFixture;

namespace Take.Elephant.Tests
{
    public abstract class KeyValueMapQueryableStorageFacts<TKey, TValue> : QueryableStorageFacts<KeyValuePair<TKey, TValue>>
    {
        public abstract IMap<TKey, TValue> Create();

        public override async Task<IQueryableStorage<KeyValuePair<TKey, TValue>>> CreateAsync(params KeyValuePair<TKey, TValue>[] values)
        {
            var map = Create();
            if (!(map is IQueryableStorage<KeyValuePair<TKey, TValue>>)) throw new ArgumentException("The map type is not an IQueryableStorage instance");            
           
            foreach (var value in values)
            {
                await map.TryAddAsync(value.Key, value.Value);
            }

            return (IQueryableStorage<KeyValuePair<TKey, TValue>>)map;
        }
    }
}
