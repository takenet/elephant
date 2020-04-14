using System;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Specialized.NotifyWrite
{
    /// <summary>
    /// Defines a <see cref="IPropertyMap{TKey,TValue}"/> decorator that notifies on write actions.
    /// </summary>
    public sealed class NotifyWriteMap<TKey, TValue> : IPropertyMap<TKey, TValue>
    {
        private readonly IMap<TKey, TValue> _map;
        private readonly IPropertyMap<TKey, TValue> _propertyMap;
        private readonly Func<TKey, CancellationToken, Task> _writeHandler;

        public NotifyWriteMap(IMap<TKey, TValue> map, Func<TKey, CancellationToken, Task> writeHandler)
        {
            _map = map ?? throw new ArgumentNullException(nameof(map));
            _writeHandler = writeHandler ?? throw new ArgumentNullException(nameof(writeHandler));
            _propertyMap = map as IPropertyMap<TKey, TValue>;
        }

        private IPropertyMap<TKey, TValue> PropertyMap => _propertyMap ?? throw new NotSupportedException("The underlying map doesn't implement IPropertyMap");

        public async Task<bool> TryAddAsync(TKey key, TValue value, bool overwrite = false, CancellationToken cancellationToken = default)
        {
            if (await _map.TryAddAsync(key, value, overwrite, cancellationToken).ConfigureAwait(false))
            {
                await _writeHandler(key, cancellationToken).ConfigureAwait(false);
                return true;
            }

            return false;
        }
        
        public async Task<bool> TryRemoveAsync(TKey key, CancellationToken cancellationToken = default)
        {
            if (await _map.TryRemoveAsync(key, cancellationToken).ConfigureAwait(false))
            {
                await _writeHandler(key, cancellationToken).ConfigureAwait(false);
                return true;
            }

            return false;
        }

        public async Task SetPropertyValueAsync<TProperty>(TKey key, string propertyName, TProperty propertyValue, CancellationToken cancellationToken = default)
        {
            await PropertyMap.SetPropertyValueAsync(key, propertyName, propertyValue, cancellationToken).ConfigureAwait(false);
            await _writeHandler(key, cancellationToken).ConfigureAwait(false);
        }
        
        public async Task MergeAsync(TKey key, TValue value, CancellationToken cancellationToken = default)
        {
            await PropertyMap.MergeAsync(key, value, cancellationToken).ConfigureAwait(false);
            await _writeHandler(key, cancellationToken).ConfigureAwait(false);
        }
        
        public Task<bool> ContainsKeyAsync(TKey key, CancellationToken cancellationToken = default) => 
            _map.ContainsKeyAsync(key, cancellationToken);

        public Task<TValue> GetValueOrDefaultAsync(TKey key, CancellationToken cancellationToken = default) => 
            _map.GetValueOrDefaultAsync(key, cancellationToken);
        
        public Task<TProperty> GetPropertyValueOrDefaultAsync<TProperty>(TKey key, string propertyName, CancellationToken cancellationToken = default) => 
            PropertyMap.GetPropertyValueOrDefaultAsync<TProperty>(key, propertyName, cancellationToken);
    }
}