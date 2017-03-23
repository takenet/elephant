using System;
using System.Threading.Tasks;

namespace Takenet.Elephant
{

    /// <summary>
    /// Represents a group of <see cref="T"/>.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class Group<T>
    {
        public Group(string key, int total, IAsyncEnumerable<T> items)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (items == null) throw new ArgumentNullException(nameof(items));
            Key = key;
            Total = total;
            Items = items;
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