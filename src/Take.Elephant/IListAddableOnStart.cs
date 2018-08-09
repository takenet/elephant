using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Take.Elephant
{
    /// <summary>
    /// Defines a list of items that allows adding an item to start.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IListAddableOnHead<T> : IList<T>
    {
        /// <summary>
        /// Adds an item to beginning of the list.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns></returns>
        Task AddToStartAsync(T value);
    }
}