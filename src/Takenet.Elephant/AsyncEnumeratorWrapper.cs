using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Takenet.Elephant
{
    public sealed class AsyncEnumeratorWrapper<T> : IAsyncEnumerator<T>
    {
        private readonly IEnumerator<T> _enumerator;

        public AsyncEnumeratorWrapper(IEnumerator<T> enumerator)
        {
            _enumerator = enumerator;
        }

        public bool MoveNext()
        {
            return _enumerator.MoveNext();
        }

        public void Reset()
        {
            _enumerator.Reset();
        }

        public T Current => _enumerator.Current;

        object IEnumerator.Current => ((IEnumerator)_enumerator).Current;

        public void Dispose()
        {
            _enumerator.Dispose();
        }

        public Task<bool> MoveNextAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(MoveNext());
        }
    }
}