using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized
{
    public class CacheSetMap<TKey, TValue> : CacheMap<TKey, ISet<TValue>>, ISetMap<TKey, TValue>
    {
        public CacheSetMap(ISetMap<TKey, TValue> primary, ISetMap<TKey, TValue> backup, TimeSpan synchronizationTimeout)
            : base(primary, backup, synchronizationTimeout)
        {
        }

        public CacheSetMap(ISetMap<TKey, TValue> primary, ISetMap<TKey, TValue> backup, ISynchronizer<IMap<TKey, ISet<TValue>>> synchronizer)
            : base(primary, backup, synchronizer)
        {
        }
    }
}
