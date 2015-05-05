using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence
{
    public sealed class AsyncEnumerableWrapper<T> : IAsyncEnumerable<T>
    {
        private readonly IEnumerable<T> _enumerable;

        public AsyncEnumerableWrapper(IEnumerable<T> enumerable)
        {
            _enumerable = enumerable;
        }

        public Task<IAsyncEnumerator<T>> GetEnumeratorAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult<IAsyncEnumerator<T>>(new AsyncEnumeratorWrapper<T>(GetEnumerator()));
        }

        public IEnumerator<T> GetEnumerator()
        {
            return _enumerable.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable)_enumerable).GetEnumerator();
        }
    }
}