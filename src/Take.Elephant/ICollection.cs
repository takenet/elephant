using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
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
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        IAsyncEnumerable<T> AsEnumerableAsync([EnumeratorCancellation]CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets the number of items in the collection.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task<long> GetLengthAsync(CancellationToken cancellationToken = default);
    }
}
