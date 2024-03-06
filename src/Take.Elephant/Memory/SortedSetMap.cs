﻿using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Memory
{
    public class SortedSetMap<TKey, TValue> : Map<TKey, ISortedSet<TValue>>, ISortedSetMap<TKey, TValue>
    {
        public SortedSetMap()
            : base(() => new SortedSet<TValue>())
        {
        }

        /// <summary>
        /// Gets the value for the key if the exists or a new sorted set for the type, if not.
        /// If the later, the item is automatically added to the map.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="cancellationToken"></param>
        /// <returns>An existing sorted set if the key exists; otherwise, an empty sorted set.</returns>
        public Task<ISortedSet<TValue>> GetValueOrEmptyAsync(TKey key, CancellationToken cancellationToken = default)
            => InternalDictionary.GetOrAdd(key, k => ValueFactory()).AsCompletedTask();
    }
}