using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Take.Elephant
{
    /// <summary>
    /// Defines a collection of items.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface ICollection<T>
    {
        /// <summary>
        /// Gets an IEnumerable with the values of the collection.
        /// </summary>
        /// <returns></returns>
        Task<IAsyncEnumerable<T>> AsEnumerableAsync();

        /// <summary>
        /// Gets the number of items in the collection.
        /// </summary>
        /// <returns></returns>
        Task<long> GetLengthAsync();
    }
}
