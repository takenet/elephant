using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Take.Elephant
{
    public static class SetExtensions
    {
        /// <summary>
        /// Gets a list from the set.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="set">The set.</param>
        /// <returns></returns>
        public static async Task<List<T>> ToListAsync<T>(this ISet<T> set)
        {
            var enumerable = set.AsEnumerableAsync();
            try
            {
                return await set.AsEnumerableAsync().ToListAsync().ConfigureAwait(false);
            }
            finally
            {
                (enumerable as IDisposable)?.Dispose();
            }
        }
    }
}
