using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Specialized.Scoping
{
    public class ScopedMap<TKey, TValue> : IPropertyMap<TKey, TValue>, IKeysMap<TKey, TValue>, IQueryableStorage<TValue>, IQueryableStorage<KeyValuePair<TKey, TValue>>, IKeyQueryableMap<TKey, TValue>, IScopedMap, IDisposable
    {
        protected readonly IMap<TKey, TValue> Map;
        protected readonly MapScope Scope;
        protected readonly ISerializer<TKey> KeySerializer;

        public ScopedMap(IMap<TKey, TValue> map, IScope scope, string identifier, ISerializer<TKey> keySerializer)
        {            
            if (scope == null) throw new ArgumentNullException(nameof(scope));
            if (identifier == null) throw new ArgumentNullException(nameof(identifier));
            if (!(scope is MapScope)) throw new ArgumentException("The provided scope type is incompatible", nameof(scope));
            if (map == null) throw new ArgumentNullException(nameof(map));
            if (keySerializer == null) throw new ArgumentNullException(nameof(keySerializer));
            Map = map;
            Scope = (MapScope)scope;
            Identifier = identifier;
            Scope.Register(this);
            KeySerializer = keySerializer;            
        }

        public virtual async Task<bool> TryAddAsync(TKey key,
            TValue value,
            bool overwrite = false,
            CancellationToken cancellationToken = default)
        {
            if (!await Map.TryAddAsync(key, value, overwrite, cancellationToken).ConfigureAwait(false)) return false;
            await Scope.AddKeyAsync(Identifier, KeySerializer.Serialize(key)).ConfigureAwait(false);
            return true;
        }

        public virtual Task<TValue> GetValueOrDefaultAsync(TKey key, CancellationToken cancellationToken = default) => 
            Map.GetValueOrDefaultAsync(key);

        public virtual async Task<bool> TryRemoveAsync(TKey key, CancellationToken cancellationToken = default)
        {
            if (!await Map.TryRemoveAsync(key, cancellationToken).ConfigureAwait(false)) return false;
            await Scope.RemoveKeyAsync(Identifier, KeySerializer.Serialize(key)).ConfigureAwait(false);
            return true;
        }

        public virtual Task<bool> ContainsKeyAsync(TKey key, CancellationToken cancellationToken = default) => 
            Map.ContainsKeyAsync(key);

        public virtual Task SetPropertyValueAsync<TProperty>(TKey key, string propertyName, TProperty propertyValue) => 
            CastMapOrThrow<IPropertyMap<TKey, TValue>>().SetPropertyValueAsync(key, propertyName, propertyValue);

        public virtual Task<TProperty> GetPropertyValueOrDefaultAsync<TProperty>(TKey key, string propertyName) => 
            CastMapOrThrow<IPropertyMap<TKey, TValue>>().GetPropertyValueOrDefaultAsync<TProperty>(key, propertyName);

        public virtual async Task MergeAsync(TKey key, TValue value)
        {
            await CastMapOrThrow<IPropertyMap<TKey, TValue>>().MergeAsync(key, value).ConfigureAwait(false);
            await Scope.AddKeyAsync(Identifier, KeySerializer.Serialize(key)).ConfigureAwait(false);
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
            var map = Map as T;
            if (map == null) throw new NotSupportedException("The underlying map doesn't support the required operation");
            return map;
        }

        public string Identifier { get; }

        public virtual Task RemoveKeyAsync(string key)
        {
            return Map.TryRemoveAsync(KeySerializer.Deserialize(key));
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                Scope.Unregister(Identifier);                
            }
        }
    }
}