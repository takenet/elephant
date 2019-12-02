using System;
using System.Collections.Generic;
using System.Threading;

namespace Take.Elephant
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

        public int Total { get; }
        
        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken)
        {
            return Items.GetAsyncEnumerator(cancellationToken);
        }
        
        public void Dispose()
        {            
            (Items as IDisposable)?.Dispose();
        }
    }
}