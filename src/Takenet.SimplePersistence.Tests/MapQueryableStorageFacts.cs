using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Ploeh.AutoFixture;

namespace Takenet.SimplePersistence.Tests
{
    public abstract class MapQueryableStorageFacts<TKey, TValue> : QueryableStorageFacts<TValue>
    {
        public abstract IMap<TKey, TValue> Create();

        public virtual TKey CreateKey()
        {
            return Fixture.Create<TKey>();
        }

        public override async Task<IQueryableStorage<TValue>> CreateAsync(params TValue[] values)
        {
            var map = Create();
            if (!(map is IQueryableStorage<TValue>)) throw new ArgumentException("The map type is not an IQueryableStorage instance");            
           
            foreach (var value in values)
            {
                var key = CreateKey();
                await map.TryAddAsync(key, value);
            }

            return (IQueryableStorage<TValue>)map;
        }
    }
}
