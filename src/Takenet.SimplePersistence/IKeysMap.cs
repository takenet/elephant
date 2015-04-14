using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence
{
    /// <summary>
    /// Defines a map service that provides direct access to the stored keys.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public interface IKeysMap<TKey, TValue> : IMap<TKey, TValue>
    {
        /// <summary>
        /// Gets all the keys stored in the map.
        /// </summary>
        /// <returns></returns>
        Task<IAsyncEnumerable<TKey>> GetKeysAsync();
    }
}
