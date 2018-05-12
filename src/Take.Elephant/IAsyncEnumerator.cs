using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant
{
    /// <summary>
    /// Defines a generic asynchronous iterator.
    /// </summary>
    public interface IAsyncEnumerator<out T> : IAsyncEnumerator, IEnumerator<T>
    {

    }

    public interface IAsyncEnumerator : IEnumerator, IDisposable
    {
        /// <summary>
        /// Advances the enumerator to the next element of the collection.
        /// </summary>
        /// <returns>
        /// true if the enumerator was successfully advanced to the next element; false if the enumerator has passed the end of the collection.
        /// </returns>
        /// <exception cref="T:System.InvalidOperationException">The collection was modified after the enumerator was created. </exception><filterpriority>2</filterpriority>
        Task<bool> MoveNextAsync(CancellationToken cancellationToken);
    }
}
