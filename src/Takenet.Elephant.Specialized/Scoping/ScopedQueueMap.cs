using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized.Scoping
{
    public class ScopedQueueMap<TKey, TItem> : ScopedMap<TKey, IQueue<TItem>>, IQueueMap<TKey, TItem>
    {
        public ScopedQueueMap(IQueueMap<TKey, TItem> map, IScope scope, string identifier, ISerializer<TKey> keySerializer) 
            : base(map, scope, identifier, keySerializer)
        {

        }
    }
}
