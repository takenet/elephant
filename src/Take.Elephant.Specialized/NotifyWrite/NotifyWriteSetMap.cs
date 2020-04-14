using System;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Specialized.NotifyWrite
{
    /// <summary>
    /// Defines a <see cref="ISetMap{TKey,TItem}"/> decorator that notifies on write actions.
    /// </summary>
    public sealed class NotifyWriteSetMap<TKey, TValue> : ISetMap<TKey, TValue>
    {
        private readonly ISetMap<TKey, TValue> _setMap;
        private readonly NotifyWriteMap<TKey, ISet<TValue>> _notifyWriteMap;
        private readonly Func<TKey, CancellationToken, Task> _writeHandler;

        public NotifyWriteSetMap(ISetMap<TKey, TValue> setMap, Func<TKey, CancellationToken, Task> writeHandler)
        {
            _setMap = setMap ?? throw new ArgumentNullException(nameof(setMap));
            _writeHandler = writeHandler ?? throw new ArgumentNullException(nameof(writeHandler));
            _notifyWriteMap = new NotifyWriteMap<TKey, ISet<TValue>>(setMap, writeHandler);
        }

        public Task<bool> TryAddAsync(TKey key, ISet<TValue> value, bool overwrite = false, CancellationToken cancellationToken = default) => 
            _notifyWriteMap.TryAddAsync(key, value, overwrite, cancellationToken);
        
        public Task<bool> TryRemoveAsync(TKey key, CancellationToken cancellationToken = default) => 
            _notifyWriteMap.TryRemoveAsync(key, cancellationToken);

        public Task<bool> ContainsKeyAsync(TKey key, CancellationToken cancellationToken = default) => 
            _notifyWriteMap.ContainsKeyAsync(key, cancellationToken);

        public async Task<ISet<TValue>> GetValueOrEmptyAsync(TKey key, CancellationToken cancellationToken = default)
        {
            var set = await _setMap.GetValueOrEmptyAsync(key, cancellationToken).ConfigureAwait(false);
            return new NotifyWriteSet<TValue>(set, (_, token) => _writeHandler(key, token));
        }
        
        public async Task<ISet<TValue>> GetValueOrDefaultAsync(TKey key, CancellationToken cancellationToken = default)
        {
            var set = await _notifyWriteMap.GetValueOrDefaultAsync(key, cancellationToken).ConfigureAwait(false);
            return set == default 
                ? default(ISet<TValue>) 
                : new NotifyWriteSet<TValue>(set, (_, token) => _writeHandler(key, token));
        }
    }
}