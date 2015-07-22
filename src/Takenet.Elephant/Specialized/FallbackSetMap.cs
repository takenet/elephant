using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized
{    
    /// <summary>
    /// Defines a fall back mechanism with a primary and secondary maps. 
    /// For write actions, the operation must succeed in both;
    /// For queries, if the action fails in the first, it falls back to the second.
    /// </summary>    
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public class FallbackSetMap<TKey, TValue> : FallbackMap<TKey, ISet<TValue>>, ISetMap<TKey, TValue>
    {
        public FallbackSetMap(ISetMap<TKey, TValue> primary, ISetMap<TKey, TValue> secondary) : base(primary, secondary)
        {
        }
    }
}
