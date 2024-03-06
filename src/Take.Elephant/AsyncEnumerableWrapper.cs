using System;
using System.Collections.Generic;
using System.Threading;

namespace Take.Elephant
{
    [Obsolete("Use 'yield return' or 'ToAsyncEnumerable' instead")]
    public sealed class AsyncEnumerableWrapper<T> : IAsyncEnumerable<T>
    {
        private readonly IEnumerable<T> _enumerable;

        public AsyncEnumerableWrapper(IEnumerable<T> enumerable)
        {
            _enumerable = enumerable;
        }

        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken)
        {
            return new AsyncEnumeratorWrapper<T>(GetEnumerator());
        }

        public IEnumerator<T> GetEnumerator()
        {
            return _enumerable.GetEnumerator();
        }
    }
}