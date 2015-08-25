using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized
{
    public class CacheSetMap<TKey, TValue> : CacheMap<TKey, ISet<TValue>>, ISetMap<TKey, TValue>
    {
        public CacheSetMap(ISetMap<TKey, TValue> source, ISetMap<TKey, TValue> cache, TimeSpan synchronizationTimeout)
            : base(source, cache, synchronizationTimeout)
        {
        }

        public CacheSetMap(ISetMap<TKey, TValue> source, ISetMap<TKey, TValue> cache, ISynchronizer<IMap<TKey, ISet<TValue>>> synchronizer)
            : base(source, cache, synchronizer)
        {
        }
    }
}
