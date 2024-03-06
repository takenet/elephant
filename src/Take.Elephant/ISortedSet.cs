﻿using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant
{
    /// <summary>
    /// Defines a set of unique items, associated with scores, that is used to take the sorted set ordered,
    /// from the smallest to the greatest score.
    /// The items are unique, scores may be repeated.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface ISortedSet<T> : IPrioritySet<T>, ICollection<T>
    {
        /// <summary>
        /// Remove value of the sorted set.
        /// </summary>
        /// <param name="value"></param>
        /// <param name="cancellationToken"></param>
        /// <returns>The number of removed items</returns>
        Task<bool> RemoveAsync(T value, CancellationToken cancellationToken = default);

        /// <summary>
        /// Returns the specified range of elements in the sorted set stored at key. By default
        /// the elements are considered to be ordered from the lowest to the highest score.
        /// Lexicographical order is used for elements with equal score. Both start and stop
        /// are zero-based indexes, where 0 is the first element, 1 is the next element and
        /// so on. They can also be negative numbers indicating offsets from the end of the
        /// sorted set, with -1 being the last element of the sorted set, -2 the penultimate
        /// element and so on.
        /// </summary>
        /// <param name="initial"></param>
        /// <param name="end"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        IAsyncEnumerable<T> GetRangeByRankAsync(long initial = 0, long end = -1, [EnumeratorCancellation]CancellationToken cancellationToken = default);

        /// <summary>
        ///  Returns the specified range of elements in the sorted set stored at key. By default
        ///  the elements are considered to be ordered from the lowest to the highest score.
        ///  Lexicographical order is used for elements with equal score. Start and stop are
        ///  used to specify the min and max range for score values. Similar to other range
        ///  methods the values are inclusive.
        /// </summary>
        /// <param name="start"></param>
        /// <param name="stop"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        IAsyncEnumerable<T> GetRangeByScoreAsync(double start = 0, double stop = 0, [EnumeratorCancellation]CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets an IEnumerable with the keyValuePair with the values and respective score on the collection.
        /// </summary>
        /// <returns></returns>
        IAsyncEnumerable<KeyValuePair<double, T>> AsEnumerableWithScoreAsync([EnumeratorCancellation]CancellationToken cancellationToken = default);
    }
}