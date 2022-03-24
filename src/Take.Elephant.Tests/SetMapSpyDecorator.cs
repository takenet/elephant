using System;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Tests
{
    public class SetMapSpyDecorator<TKey, TValue> : ISetMap<TKey, TValue>
    {
        private readonly ISetMap<TKey, TValue> _setMap;

        public SetMapSpyDecorator(ISetMap<TKey, TValue> setMap)
        {
            _setMap = setMap;
        }

        public int ReadCount { get; private set; }
        public int WriteCount { get; private set; }

        public Task<bool> ContainsKeyAsync(TKey key, CancellationToken cancellationToken = default)
        {
            return _setMap.ContainsKeyAsync(key, cancellationToken).ContinueWith(t => { if (!t.IsFaulted) ReadCount++; return t.Result; });
        }

        public Task<ISet<TValue>> GetValueOrDefaultAsync(TKey key, CancellationToken cancellationToken = default)
        {
            return _setMap.GetValueOrDefaultAsync(key, cancellationToken).ContinueWith(t => { if (!t.IsFaulted) ReadCount++; return t.Result; });
        }

        public Task<ISet<TValue>> GetValueOrEmptyAsync(TKey key, CancellationToken cancellationToken = default)
        {
            return _setMap.GetValueOrEmptyAsync(key, cancellationToken).ContinueWith(t => { if (!t.IsFaulted) ReadCount++; return t.Result; });
        }

        public Task<bool> TryAddAsync(TKey key, ISet<TValue> value, bool overwrite = false, CancellationToken cancellationToken = default)
        {
            return _setMap.TryAddAsync(key, value, overwrite, cancellationToken).ContinueWith(t => { if (!t.IsFaulted) WriteCount++; return t.Result; });
        }

        public Task<bool> TryRemoveAsync(TKey key, CancellationToken cancellationToken = default)
        {
            return _setMap.TryRemoveAsync(key, cancellationToken).ContinueWith(t => { if (!t.IsFaulted) WriteCount++; return t.Result; });
        }
    }
}