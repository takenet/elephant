using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Take.Elephant.Memory;

namespace Take.Elephant
{
    public static class EnumerableExtensions
    {
        /// <summary>
        /// Creates a set from the enumerable.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="enumerable">The enumerable.</param>
        /// <returns></returns>
        public static ISet<T> ToSet<T>(this IEnumerable<T> enumerable)
        {
            return new Set<T>(enumerable);
        }

        /// <summary>
        /// Creates a set from the enumerable.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="enumerable">The enumerable.</param>
        /// <param name="equalityComparer">The equality comparer.</param>
        /// <returns></returns>
        public static ISet<T> ToSet<T>(this IEnumerable<T> enumerable, IEqualityComparer<T> equalityComparer)
        {
            return new Set<T>(enumerable, equalityComparer);
        }
    }
}
