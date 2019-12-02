using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Take.Elephant
{

    /// <summary>
    /// Represents a group of <see cref="T"/>.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class Group<T>
    {
        public Group(string key, int total, IAsyncEnumerable<T> items)
        {
            Key = key ?? throw new ArgumentNullException(nameof(key));
            Total = total;
            Items = items ?? throw new ArgumentNullException(nameof(items));
        }

        /// <summary>
        /// Gets the group key.
        /// </summary>
        public string Key { get; }

        /// <summary>
        /// Gets the total of items in the group.
        /// </summary>
        public int Total { get; }

        /// <summary>
        /// Gets the group items.
        /// </summary>
        public IAsyncEnumerable<T> Items { get; }
    }
}