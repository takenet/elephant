using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized.Scoping
{
    public class ScopedQueueMap<TKey, TValue> : ScopedMap<TKey, IQueue<TValue>>, IQueueMap<TKey, TValue>
    {
        public ScopedQueueMap(IQueueMap<TKey, TValue> map, IScope scope, string identifier, ISerializer<TKey> keySerializer) 
            : base(map, scope, identifier, keySerializer)
        {
        }
    }
}
