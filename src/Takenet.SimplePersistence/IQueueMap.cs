using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence
{
    /// <summary>
    /// Represents a map that contains a queue of items.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    ///0 0< typeparam name="TItem"></typeparam>

    public interface IQueueMap<TKey, TItem> : IMap<TKey, IQueue<TItem>>
    {
    }
}
