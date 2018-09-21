using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Take.Elephant
{
    /// <summary>
    /// Defines a list of items that allows duplicate values.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IList<T> : ICollection<T>
    {
        /// <summary>
        /// Adds an item to the end of the list.         
        /// </summary>
        /// <param name="value">The value.</param>        
        /// <returns></returns>
        Task AddAsync(T value);

        /// <summary>
        /// Remove all occurrences of the item in the list.
        /// </summary>
        /// <param name="value"></param>
        /// <returns>The number of removed items</returns>
        Task<long> RemoveAllAsync(T value);
    }
}
