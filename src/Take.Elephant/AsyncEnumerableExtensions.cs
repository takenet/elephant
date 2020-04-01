using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant
{
    public static class AsyncEnumerableExtensions
    {
        // <summary>
        // Asynchronously executes the provided action on each element of the <see cref="IAsyncEnumerable{T}" />.
        // </summary>
        // <param name="func"> The action to be executed. </param>
        // <param name="cancellationToken"> The token to monitor for cancellation requests. </param>
        // <returns> A Task representing the asynchronous operation. </returns>
        [Obsolete("Use ForEachAwaitAsync from System.Linq.AsyncEnumerable")]
        public static Task ForEachAsync<T>(
            this IAsyncEnumerable<T> source, 
            Func<T, Task> func, 
            CancellationToken cancellationToken = default) =>
            AsyncEnumerable.ForEachAwaitAsync(source, func, cancellationToken);
    }
}
