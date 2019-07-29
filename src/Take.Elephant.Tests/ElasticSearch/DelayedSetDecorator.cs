using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Tests.ElasticSearch
{
    internal class DelayedSetDecorator<T> : ISet<T>
    {
        private readonly ISet<T> _set;

        public DelayedSetDecorator(ISet<T> set)
        {
            _set = set;
        }
        public Task AddAsync(T value, CancellationToken cancellationToken = default)
        {
            Thread.Sleep(1000);
            return _set.AddAsync(value, cancellationToken);
        }

        public Task<IAsyncEnumerable<T>> AsEnumerableAsync(CancellationToken cancellationToken = default)
        {
            return _set.AsEnumerableAsync(cancellationToken);
        }

        public Task<bool> ContainsAsync(T value, CancellationToken cancellationToken = default)
        {
            Thread.Sleep(1000);
            return _set.ContainsAsync(value, cancellationToken);
        }

        public Task<long> GetLengthAsync(CancellationToken cancellationToken = default)
        {
            Thread.Sleep(1000);
            return _set.GetLengthAsync(cancellationToken);
        }

        public Task<bool> TryRemoveAsync(T value, CancellationToken cancellationToken = default)
        {
            Thread.Sleep(1000);
            return _set.TryRemoveAsync(value, cancellationToken);
        }
    }
}
