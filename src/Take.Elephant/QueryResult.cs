using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Take.Elephant
{
    /// <summary>
    /// Represents a query result.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public sealed class QueryResult<T> : IAsyncEnumerable<T>, IEnumerable<T>, IDisposable
    {
        public QueryResult(IEnumerable<T> items, int total)
            : this(items?.ToAsyncEnumerable(), total)
        {
            
        }
        
        public QueryResult(IAsyncEnumerable<T> items, int total)
        {
            Items = items ?? AsyncEnumerable.Empty<T>();
            Total = total;
        }

        public IAsyncEnumerable<T> Items { get; }

        public int Total { get; }
        
        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken)
        {
            return Items.GetAsyncEnumerator(cancellationToken);
        }
        
        [Obsolete]
        public IEnumerator<T> GetEnumerator()
        {
            return Items.ToEnumerable().GetEnumerator();
        }

        [Obsolete]
        IEnumerator IEnumerable.GetEnumerator()
        {
            return Items.ToEnumerable().GetEnumerator();
        }
        
        public void Dispose()
        {            
            (Items as IDisposable)?.Dispose();
        }
    }
}