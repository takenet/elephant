using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Takenet.Elephant
{
    /// <summary>
    /// Represents a query result.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public sealed class QueryResult<T> : IAsyncEnumerable<T>, IDisposable
    {
        public QueryResult(IAsyncEnumerable<T> items, int total)
        {
            Items = items;
            Total = total;
        }

        public IAsyncEnumerable<T> Items { get; }

        public int Total { get; private set; }


        public Task<IAsyncEnumerator<T>> GetEnumeratorAsync(CancellationToken cancellationToken)
        {
            return Items.GetEnumeratorAsync(cancellationToken);
        }

        public IEnumerator<T> GetEnumerator()
        {
            return Items.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable)Items).GetEnumerator();
        }

        public void Dispose()
        {            
            (Items as IDisposable)?.Dispose();
        }

    }
}