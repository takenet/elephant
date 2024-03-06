using System.Collections.Generic;
using System.Threading.Tasks;

namespace Take.Elephant
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
        
        public async ValueTask<bool> MoveNextAsync()
        {
            return MoveNext();
        }

        public ValueTask DisposeAsync()
        {
            _enumerator.Dispose();
            return default;
        }
    }
}