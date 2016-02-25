using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized.Scoping
{
    public class ScopedQueueMap<TKey, TValue> : ScopedMap<TKey, IQueue<TValue>>, IQueueMap<TKey, TValue>
    {
        public ScopedQueueMap(IMap<TKey, IQueue<TValue>> map, IScope scope, ISerializer<TKey> keySerializer) 
            : base(map, scope, keySerializer)
        {
        }
    }
}
