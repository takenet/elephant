using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized.Scoping
{
    public class ScopedMap<TKey, TValue> : IPropertyMap<TKey, TValue>, IKeysMap<TKey, TValue>, IQueryableStorage<TValue>, IQueryableStorage<KeyValuePair<TKey, TValue>>, IKeyQueryableMap<TKey, TValue>
    {
        private readonly IMap<TKey, TValue> _map;
        private readonly MapScope _scope;
        private readonly ISerializer<TKey> _keySerializer;

        public ScopedMap(IMap<TKey, TValue> map, IScope scope, ISerializer<TKey> keySerializer)
        {
            if (scope == null) throw new ArgumentNullException(nameof(scope));
            if (!(scope is MapScope)) throw new ArgumentException("The provided scope type is incompatible", nameof(scope));
            if (map == null) throw new ArgumentNullException(nameof(map));
            if (keySerializer == null) throw new ArgumentNullException(nameof(keySerializer));
            _map = map;
            _scope = (MapScope)scope;
            _scope.RemoveKeyFunc = k => _map.TryRemoveAsync(_keySerializer.Deserialize(k));            
            _keySerializer = keySerializer;
        }

        public virtual async Task<bool> TryAddAsync(TKey key, TValue value, bool overwrite = false)
        {
            if (!await _map.TryAddAsync(key, value, overwrite).ConfigureAwait(false)) return false;
            await _scope.AddKeyAsync(_keySerializer.Serialize(key)).ConfigureAwait(false);
            return true;
        }

        public virtual Task<TValue> GetValueOrDefaultAsync(TKey key) => 
            _map.GetValueOrDefaultAsync(key);

        public virtual async Task<bool> TryRemoveAsync(TKey key)
        {
            if (!await _map.TryRemoveAsync(key).ConfigureAwait(false)) return false;
            await _scope.RemoveKeyAsync(_keySerializer.Serialize(key)).ConfigureAwait(false);
            return true;
        }

        public virtual Task<bool> ContainsKeyAsync(TKey key) => 
            _map.ContainsKeyAsync(key);

        public virtual Task SetPropertyValueAsync<TProperty>(TKey key, string propertyName, TProperty propertyValue) => 
            CastMapOrThrow<IPropertyMap<TKey, TValue>>().SetPropertyValueAsync(key, propertyName, propertyValue);

        public virtual Task<TProperty> GetPropertyValueOrDefaultAsync<TProperty>(TKey key, string propertyName) => 
            CastMapOrThrow<IPropertyMap<TKey, TValue>>().GetPropertyValueOrDefaultAsync<TProperty>(key, propertyName);

        public virtual async Task MergeAsync(TKey key, TValue value)
        {
            await CastMapOrThrow<IPropertyMap<TKey, TValue>>().MergeAsync(key, value).ConfigureAwait(false);
            await _scope.AddKeyAsync(_keySerializer.Serialize(key)).ConfigureAwait(false);
        }

        public virtual Task<IAsyncEnumerable<TKey>> GetKeysAsync() =>
            CastMapOrThrow<IKeysMap<TKey, TValue>>().GetKeysAsync();

        public virtual Task<QueryResult<TValue>> QueryAsync<TResult>(Expression<Func<TValue, bool>> @where, Expression<Func<TValue, TResult>> @select, int skip, int take, CancellationToken cancellationToken) =>
            CastMapOrThrow<IQueryableStorage<TValue>>().QueryAsync(@where, @select, skip, take, cancellationToken);

        public virtual Task<QueryResult<KeyValuePair<TKey, TValue>>> QueryAsync<TResult>(Expression<Func<KeyValuePair<TKey, TValue>, bool>> @where, Expression<Func<KeyValuePair<TKey, TValue>, TResult>> @select, int skip, int take, CancellationToken cancellationToken) =>
            CastMapOrThrow<IQueryableStorage<KeyValuePair<TKey, TValue>>>().QueryAsync(@where, @select, skip, take, cancellationToken);

        public virtual Task<QueryResult<TKey>> QueryForKeysAsync<TResult>(Expression<Func<TValue, bool>> @where, Expression<Func<TKey, TResult>> @select, int skip, int take, CancellationToken cancellationToken) =>
            CastMapOrThrow<IKeyQueryableMap<TKey, TValue>>().QueryForKeysAsync(@where, @select, skip, take, cancellationToken);

        protected virtual T CastMapOrThrow<T>() where T : class
        {
            var map = _map as T;
            if (map == null) throw new NotSupportedException("The underlying map doesn't support the required operation");
            return map;
        }
    }
}