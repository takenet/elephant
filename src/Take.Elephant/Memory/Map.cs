using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using Timer = System.Timers.Timer;

namespace Take.Elephant.Memory
{
    /// <summary>
    /// Implements the <see cref="IMap{TKey,TValue}"/> interface using the <see cref="System.Collections.Concurrent.ConcurrentDictionary{TKey,TValue}"/> class.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public class Map<TKey, TValue> : IUpdatableMap<TKey, TValue>, IExpirableKeyMap<TKey, TValue>, IPropertyMap<TKey, TValue>, IKeysMap<TKey, TValue>, IQueryableStorage<TValue>, IOrderedQueryableStorage<TValue>, IQueryableStorage<KeyValuePair<TKey, TValue>>, IKeyQueryableMap<TKey, TValue>, IDisposable
    {
        private readonly Timer _expirationTimer;
        private readonly double _expirationTimerIntervalMs;

        public Map(TimeSpan expirationScanInterval = default)
            : this(() => (TValue)Activator.CreateInstance(typeof(TValue)), expirationScanInterval)
        {
        }

        public Map(Func<TValue> valueFactory, TimeSpan expirationScanInterval = default)
            : this(valueFactory, new DictionaryConverter<TValue>(valueFactory), expirationScanInterval)
        {
        }

        public Map(IDictionaryConverter<TValue> dictionaryConverter, TimeSpan expirationScanInterval = default)
            : this(() => (TValue)Activator.CreateInstance(typeof(TValue)), dictionaryConverter, expirationScanInterval)
        {
        }

        public Map(Func<TValue> valueFactory, IDictionaryConverter<TValue> dictionaryConverter, TimeSpan expirationScanInterval = default)
            : this(valueFactory, dictionaryConverter, new ConcurrentDictionary<TKey, TValue>(), expirationScanInterval)
        {
        }

        public Map(Func<TValue> valueFactory, IDictionaryConverter<TValue> dictionaryConverter, ConcurrentDictionary<TKey, TValue> internalDictionary, TimeSpan expirationScanInterval = default)
        {
            ValueFactory = valueFactory ?? throw new ArgumentNullException(nameof(valueFactory));
            DictionaryConverter = dictionaryConverter ?? throw new ArgumentNullException(nameof(dictionaryConverter));
            InternalDictionary = internalDictionary ?? throw new ArgumentNullException(nameof(internalDictionary));
            KeyExpirationDictionary = new ConcurrentDictionary<TKey, DateTimeOffset>();
            if (expirationScanInterval <= TimeSpan.Zero)
            {
                expirationScanInterval = TimeSpan.FromSeconds(30);
            }
            _expirationTimerIntervalMs = expirationScanInterval.TotalMilliseconds; 
            _expirationTimer = new Timer(_expirationTimerIntervalMs);
            _expirationTimer.Elapsed += RemoveExpiredKeys;
        }

        /// <summary>
        /// Enable/disable fetching of total record count on Queries.
        /// Default: Enabled.
        /// </summary>
        public bool FetchQueryResultTotal { get; set; } = true;

        protected Func<TValue> ValueFactory { get; }

        protected IDictionaryConverter<TValue> DictionaryConverter { get; }

        protected ConcurrentDictionary<TKey, TValue> InternalDictionary { get; }
        
        protected ConcurrentDictionary<TKey, DateTimeOffset> KeyExpirationDictionary { get; }

        public virtual Task<bool> TryAddAsync(
            TKey key,
            TValue value,
            bool overwrite = false,
            CancellationToken cancellationToken = default)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            bool added;
            
            if (overwrite)
            {
                InternalDictionary.AddOrUpdate(key, value, (k, v) => value);
                added = true;
            }
            else
            {
                added = InternalDictionary.TryAdd(key, value);
            }

            if (added)
            {
                KeyExpirationDictionary.TryRemove(key, out _);
            }

            return Task.FromResult(added);
        }

        public virtual Task<TValue> GetValueOrDefaultAsync(TKey key, CancellationToken cancellationToken = default)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            TryGetValue(key, out var value);
            return Task.FromResult(value);
        }

        public virtual Task<bool> TryRemoveAsync(TKey key, CancellationToken cancellationToken = default)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            var removed = InternalDictionary.TryRemove(key, out _);
            if (removed)
            {
                KeyExpirationDictionary.TryRemove(key, out _);
            }
            return Task.FromResult(removed);
        }

        public virtual Task<bool> ContainsKeyAsync(TKey key, CancellationToken cancellationToken = default)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            var contains = InternalDictionary.ContainsKey(key) && !KeyHasExpired(key);
            return Task.FromResult(contains);
        }

        public virtual Task<bool> TryUpdateAsync(TKey key, TValue newValue, TValue oldValue)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            bool updated;
            if (InternalDictionary.TryUpdate(key, newValue, oldValue))
            {
                KeyExpirationDictionary.TryRemove(key, out _);
                updated = true;
            }
            else
            {
                updated = false;
            }

            return Task.FromResult(updated);
        }

        public virtual Task SetRelativeKeyExpirationAsync(TKey key, TimeSpan ttl)
        {
            return SetAbsoluteKeyExpirationAsync(key, DateTimeOffset.UtcNow.Add(ttl));
        }

        public virtual Task SetAbsoluteKeyExpirationAsync(TKey key, DateTimeOffset expiration)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (!InternalDictionary.ContainsKey(key))
            {
                throw new ArgumentException("$Key {key}'' not found");
            }

            if (!_expirationTimer.Enabled)
            {
                // The start method ignores multiple calls so it is thread-safe
                _expirationTimer.Start();
            }

            KeyExpirationDictionary[key] = expiration;
            return Task.CompletedTask;
        }

        public virtual Task SetPropertyValueAsync<TProperty>(TKey key, string propertyName, TProperty propertyValue)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            TValue value = GetOrCreateValue(key);
            var property = typeof(TValue).GetProperty(
                propertyName,
                BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);

            if (property == null) throw new ArgumentException("The property name is invalid", nameof(propertyName));
            property.SetValue(value, propertyValue);

            return Task.FromResult<object>(null);
        }

        public virtual Task<TProperty> GetPropertyValueOrDefaultAsync<TProperty>(TKey key, string propertyName)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            TValue value;
            var property = typeof(TValue).GetProperty(
                propertyName,
                BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);

            if (property == null) throw new ArgumentException("The property name is invalid", nameof(propertyName));
            var propertyValue = default(TProperty);
            if (InternalDictionary.TryGetValue(key, out value))
            { 
                propertyValue = (TProperty) property.GetValue(value);
            }
            return Task.FromResult(propertyValue);
        }

        public virtual Task MergeAsync(TKey key, TValue value)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (value == null) throw new ArgumentNullException(nameof(value));

            var properties = DictionaryConverter.ToDictionary(value);
            if (!properties.Any()) return TaskUtil.CompletedTask;
            var existingValue = GetOrCreateValue(key);

            foreach (var propertyKeyValue in properties)
            {
                var property = typeof(TValue)
                    .GetProperty(
                        propertyKeyValue.Key,
                        BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);

                if (property != null)
                {
                    try
                    {
                        if (property.PropertyType.GetTypeInfo().IsEnum)
                        {
                            property.SetValue(
                                existingValue,
                                Enum.Parse(property.PropertyType, propertyKeyValue.Value.ToString(), true));
                        }
                        else
                        {
                            property.SetValue(
                                existingValue,
                                propertyKeyValue.Value);
                        }
                    }
                    catch
                    {
                        var parse = TypeUtil.GetParseFuncForType(property.PropertyType);
                        if (parse != null)
                        {
                            property.SetValue(existingValue, parse(propertyKeyValue.Value.ToString()));
                        }
                        else
                        {
                            throw new InvalidOperationException($"Cannot set value for property '{property.Name}'");
                        }
                    }
                }
            }

            return Task.CompletedTask;
        }

        public virtual Task<IAsyncEnumerable<TKey>> GetKeysAsync()
        {
            var keys = InternalDictionary.Keys.Where(KeyHasNotExpired).ToAsyncEnumerable();
            return Task.FromResult(keys);
        }

        public virtual Task<QueryResult<TKey>> QueryForKeysAsync<TResult>(
            Expression<Func<TValue, bool>> @where,
            Expression<Func<TKey, TResult>> @select,
            int skip,
            int take,
            CancellationToken cancellationToken)
        {
            if (@where == null) @where = value => true;
            if (select != null &&
                select.ReturnType != typeof(TKey))
            {
                throw new NotImplementedException("The select parameter is not supported yet");
            }
            var totalValues = InternalDictionary
                .Where(pair => KeyHasNotExpired(pair.Key) && where.Compile().Invoke(pair.Value));
            
            var totalCount = 0;
            if (FetchQueryResultTotal)
            {
                totalCount = totalValues.Count();
            }

            var resultValues = totalValues
                .Skip(skip)
                .Take(take)
                .Select(pair => pair.Key);

            var queryResult = new QueryResult<TKey>(resultValues, totalCount);
            return Task.FromResult(queryResult);
        }

        public virtual Task<QueryResult<TValue>> QueryAsync<TResult>(
            Expression<Func<TValue, bool>> @where,
            Expression<Func<TValue, TResult>> @select,
            int skip,
            int take,
            CancellationToken cancellationToken)
        {
            if (@where == null) @where = value => true;
            if (select != null && 
                select.ReturnType != typeof(TValue))
            {
                throw new NotImplementedException("The select parameter is not supported yet");
            }

            var totalValues = InternalDictionary
                .Where(pair => KeyHasNotExpired(pair.Key) && where.Compile().Invoke(pair.Value));

            var totalCount = 0;
            if (FetchQueryResultTotal)
            {
                totalCount = totalValues.Count();
            }
            
            var resultValues = totalValues
                .Skip(skip)
                .Take(take)
                .Select(pair => pair.Value);

            var queryResult = new QueryResult<TValue>(resultValues, totalCount);
            return Task.FromResult(queryResult);
        }

        public virtual Task<QueryResult<TValue>> QueryAsync<TResult, TOrderBy>(
            Expression<Func<TValue, bool>> where,
            Expression<Func<TValue, TResult>> select,
            Expression<Func<TValue, TOrderBy>> orderBy,
            bool orderByAscending,
            int skip,
            int take,
            CancellationToken cancellationToken)
        {
            if (@where == null) @where = value => true;
            if (select != null &&
                select.ReturnType != typeof(TValue))
            {
                throw new NotImplementedException("The select parameter is not supported yet");
            }

            var totalValues = InternalDictionary
                .Where(pair => KeyHasNotExpired(pair.Key))
                .Select(pair => pair.Value)
                .Where(value => where.Compile().Invoke(value));
            var orderByFunc = orderBy.Compile();
            
            int totalCount = 0;
            if (FetchQueryResultTotal)
            {
                totalCount = totalValues.Count();
            }

            var orderedTotalValues = orderByAscending 
                ? totalValues.OrderBy(orderByFunc.Invoke) 
                : totalValues.OrderByDescending(orderByFunc.Invoke);

            var resultValues = orderedTotalValues
                .Skip(skip)
                .Take(take)
                .Select(value => value);

            var queryResult = new QueryResult<TValue>(resultValues, totalCount);
            return Task.FromResult(queryResult);
        }

        public virtual Task<QueryResult<KeyValuePair<TKey, TValue>>> QueryAsync<TResult>(
            Expression<Func<KeyValuePair<TKey, TValue>, bool>> @where,
            Expression<Func<KeyValuePair<TKey, TValue>, TResult>> @select,
            int skip,
            int take,
            CancellationToken cancellationToken)
        {
            if (@where == null) @where = value => true;
            if (select != null &&
                select.ReturnType != typeof(KeyValuePair<TKey, TValue>))
            {
                throw new NotImplementedException("The select parameter is not supported yet");
            }
            var totalValues = InternalDictionary
                .Where(pair => KeyHasNotExpired(pair.Key) && where.Compile().Invoke(pair));

            int totalCount = 0;
            if (FetchQueryResultTotal)
            {
                totalCount = totalValues.Count();
            }
            
            var resultValues = totalValues
                .Skip(skip)
                .Take(take);

            var queryResult = new QueryResult<KeyValuePair<TKey, TValue>>(resultValues, totalCount);
            return Task.FromResult(queryResult);
        }

        protected TValue GetOrCreateValue(TKey key)
        {
            if (TryGetValue(key, out var value))
            {
                return value;
            }
            
            value = ValueFactory();
            InternalDictionary[key] = value;
            KeyExpirationDictionary.TryRemove(key, out _);
            return value;
        }

        protected bool TryGetValue(TKey key, out TValue value)
        {
            if (InternalDictionary.TryGetValue(key, out var candidateValue) &&
                !KeyHasExpired(key))
            {
                value = candidateValue;
                return true;
            }

            value = default;
            return false;
        }


        protected bool KeyHasNotExpired(TKey key) => !KeyHasExpired(key);
        
        protected bool KeyHasExpired(TKey key)
        {
            return KeyExpirationDictionary.TryGetValue(key, out var expiration) && 
                   expiration <= DateTimeOffset.UtcNow;
        }

        private void RemoveExpiredKeys(object sender, ElapsedEventArgs e)
        {
            // Increase the interval to avoid the timer to be triggered while processing
            // This is better than calling Start/Stop since it call Change instead of creating new Threading.Timer
            // instance internally by the Timers.Time class.
            _expirationTimer.Interval = 3_600_000; // An hour

            try
            {
                // Creates a snapshot of the expired keys
                var expiredKeys = KeyExpirationDictionary
                    .Where(pair => pair.Value <= DateTimeOffset.UtcNow)
                    .Select(pair => pair.Key)
                    .ToArray();

                foreach (var key in expiredKeys)
                {
                    // Check again to reduce the change of the collection be changed between the snapshot and now.
                    // This cleanup is not thread safe and a valid key may be removed by the process.
                    if (KeyExpirationDictionary.TryRemove(key, out var keyExpiration))
                    {
                        if (keyExpiration <= DateTimeOffset.UtcNow)
                        {
                            InternalDictionary.TryRemove(key, out _);
                        }
                        else
                        {
                            // Put it back
                            KeyExpirationDictionary[key] = keyExpiration;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Trace.TraceError(ex.ToString());
            }
            finally
            {
                _expirationTimer.Interval = _expirationTimerIntervalMs;
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _expirationTimer.Dispose();
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}