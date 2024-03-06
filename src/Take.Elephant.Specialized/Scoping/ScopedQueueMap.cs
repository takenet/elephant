using System;
using System.Threading.Tasks;

namespace Take.Elephant.Specialized.Scoping
{
    public class ScopedQueueMap<TKey, TItem> : ScopedMap<TKey, IQueue<TItem>>, IQueueMap<TKey, TItem>
    {
        public ScopedQueueMap(IQueueMap<TKey, TItem> map, IScope scope, string identifier, ISerializer<TKey> keySerializer) 
            : base(map, scope, identifier, keySerializer)
        {

        }

        public virtual Task<IQueue<TItem>> GetValueOrEmptyAsync(TKey key)
        {
            throw new NotImplementedException("This method is not yet implemented");
        }

    }
}
