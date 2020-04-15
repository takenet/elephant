using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant
{
    public static class SetExtensions
    {
        /// <summary>
        /// Creates a list from the set.
        /// </summary>
        /// <returns></returns>
        [Obsolete("Use AsEnumerable().ToListAsync() instead")]
        public static ValueTask<List<T>> ToListAsync<T>(this ISet<T> set, CancellationToken cancellationToken = default) => 
            set.AsEnumerableAsync(cancellationToken).ToListAsync(cancellationToken: cancellationToken);

        /// <summary>
        /// Creates a buffered memory set.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="set">The set.</param>
        /// <returns></returns>
        public static async Task<Memory.Set<T>> ToMemorySetAsync<T>(this ISet<T> set, CancellationToken cancellationToken = default)
        {
            var memorySet = new Memory.Set<T>();
            await foreach (var item in set.AsEnumerableAsync(cancellationToken).ConfigureAwait(false))
            {
                await memorySet.AddAsync(item, cancellationToken);
            }
            return memorySet;
        }
    }
}
