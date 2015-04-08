using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence.Memory
{
    /// <summary>
    /// Implements the <see cref="ISetMap{TKey,TItem}"/> interface using the <see cref="Map{TKey,TValue}"/> and <see cref="Set{T}"/> classes.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TItem"></typeparam>
    public class SetMap<TKey, TItem> : Map<TKey, ISet<TItem>>, ISetMap<TKey, TItem>
    {
        public SetMap()
            : base(() => new Set<TItem>())
        {
            
        }
    }
}
