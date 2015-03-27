using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence.Memory
{
    /// <summary>
    /// Implements the <see cref="ISetMap{TKey,TItem}"/> interface using the <see cref="DictionaryMap{TKey,TValue}"/> and <see cref="HashSet{T}"/> classes.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TItem"></typeparam>
    public class DictionaryHashSetMap<TKey, TItem> : DictionaryMap<TKey, ISet<TItem>>, ISetMap<TKey, TItem>
    {
        public DictionaryHashSetMap()
            : base(() => new HashSet<TItem>())
        {
            
        }
    }
}
