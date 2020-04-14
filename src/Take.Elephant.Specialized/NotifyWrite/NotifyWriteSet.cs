using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Specialized.NotifyWrite
{
    /// <summary>
    /// Defines a <see cref="ISet{T}"/> decorator that notifies on write actions.
    /// </summary>
    public sealed class NotifyWriteSet<T> : ISet<T>
    {
        private readonly ISet<T> _set;
        private readonly Func<T, CancellationToken, Task> _writeHandler;

        public NotifyWriteSet(ISet<T> set, Func<T, CancellationToken, Task> writeHandler)
        {
            _set = set ?? throw new ArgumentNullException(nameof(set));
            _writeHandler = writeHandler ?? throw new ArgumentNullException(nameof(writeHandler));
        }
        
        public async Task AddAsync(T value, CancellationToken cancellationToken = default)
        {
            await _set.AddAsync(value, cancellationToken).ConfigureAwait(false);
            await _writeHandler(value, cancellationToken).ConfigureAwait(false);
        }

        public async Task<bool> TryRemoveAsync(T value, CancellationToken cancellationToken = default)
        {
            if (await _set.TryRemoveAsync(value, cancellationToken).ConfigureAwait(false))
            {
                await _writeHandler(value, cancellationToken).ConfigureAwait(false);
                return true;
            }
            
            return false;
        }

        public IAsyncEnumerable<T> AsEnumerableAsync(CancellationToken cancellationToken = default) => 
            _set.AsEnumerableAsync(cancellationToken);

        public Task<long> GetLengthAsync(CancellationToken cancellationToken = default) => 
            _set.GetLengthAsync(cancellationToken);

        public Task<bool> ContainsAsync(T value, CancellationToken cancellationToken = default) => 
            _set.ContainsAsync(value, cancellationToken);
    }
}