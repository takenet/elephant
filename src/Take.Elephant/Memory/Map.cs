using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Memory
{
    /// <summary>
    /// Implements the <see cref="IMap{TKey,TValue}"/> interface using the <see cref="System.Collections.Concurrent.ConcurrentDictionary{TKey,TValue}"/> class.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public class Map<TKey, TValue> : IUpdatableMap<TKey, TValue>, IExpirableKeyMap<TKey, TValue>, IPropertyMap<TKey, TValue>, IKeysMap<TKey, TValue>, IQueryableStorage<TValue>, IOrderedQueryableStorage<TValue>, IQueryableStorage<KeyValuePair<TKey, TValue>>, IKeyQueryableMap<TKey, TValue>
    {
        public Map()
            : this(() => (TValue)Activator.CreateInstance(typeof(TValue)))
        {
        }

        public Map(Func<TValue> valueFactory)
            : this(valueFactory, new DictionaryConverter<TValue>(valueFactory))
        {
        }

        public Map(IDictionaryConverter<TValue> dictionaryConverter)
            : this(() => (TValue)Activator.CreateInstance(typeof(TValue)), dictionaryConverter)
        {
        }

        public Map(Func<TValue> valueFactory, IDictionaryConverter<TValue> dictionaryConverter)
            : this(valueFactory, dictionaryConverter, new ConcurrentDictionary<TKey, TValue>())
        {
        }

        public Map(Func<TValue> valueFactory, IDictionaryConverter<TValue> dictionaryConverter, ConcurrentDictionary<TKey, TValue> internalDictionary)
        {
            ValueFactory = valueFactory ?? throw new ArgumentNullException(nameof(valueFactory));
            DictionaryConverter = dictionaryConverter ?? throw new ArgumentNullException(nameof(dictionaryConverter));
            InternalDictionary = internalDictionary ?? throw new ArgumentNullException(nameof(internalDictionary));
        }

        /// <summary>
        /// Enable/disable fetching of total record count on Queries.
        /// Default: Enabled.
        /// </summary>
        public bool FetchQueryResultTotal { get; set; } = true;

        protected Func<TValue> ValueFactory { get; }

        protected IDictionaryConverter<TValue> DictionaryConverter { get; }

        protected ConcurrentDictionary<TKey, TValue> InternalDictionary { get; }

        #region IMap<TKey,TValue> Members

        public virtual Task<bool> TryAddAsync(TKey key,
            TValue value,
            bool overwrite = false,
            CancellationToken cancellationToken = default)
        {
            if (overwrite)
            {
                InternalDictionary.AddOrUpdate(key, value, (k, v) => value);
                return Task.FromResult(true);
            }

            return Task.FromResult(InternalDictionary.TryAdd(key, value));
        }

        public virtual Task<TValue> GetValueOrDefaultAsync(TKey key, CancellationToken cancellationToken = default) 
            => Task.FromResult(InternalDictionary.TryGetValue(key, out var value) ? value : default(TValue));

        public virtual Task<bool> TryRemoveAsync(TKey key, CancellationToken cancellationToken = default)
        {
            TValue value;
            return Task.FromResult(InternalDictionary.TryRemove(key, out value));
        }

        public virtual Task<bool> ContainsKeyAsync(TKey key, CancellationToken cancellationToken = default) 
            => Task.FromResult(InternalDictionary.ContainsKey(key));

        #endregion

        #region IUpdatableMap<TKey,TValue> Members

        public virtual Task<bool> TryUpdateAsync(TKey key, TValue newValue, TValue oldValue)
        {
            return InternalDictionary.TryUpdate(key, newValue, oldValue).AsCompletedTask();
        }

        #endregion

        #region IExpirableKeyMap<TKey,TValue> Members

        private readonly ConcurrentDictionary<TKey, Tuple<Task, CancellationTokenSource>> _expirationTaskDictionary = new ConcurrentDictionary<TKey, Tuple<Task, CancellationTokenSource>>();

        public virtual Task SetRelativeKeyExpirationAsync(TKey key, TimeSpan ttl)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (!InternalDictionary.ContainsKey(key)) throw new ArgumentException("Invalid key", nameof(key));

            Tuple<Task, CancellationTokenSource> expirationTaskWithCts;
            if (_expirationTaskDictionary.TryRemove(key, out expirationTaskWithCts))
            {
                expirationTaskWithCts.Item2.Cancel();
            }

            var cancellationTokenSource = new CancellationTokenSource();
            var expirationTask = Task.Run(async () =>
            {
                await Task.Delay(ttl, cancellationTokenSource.Token).ConfigureAwait(false);
                await TryRemoveAsync(key).ConfigureAwait(false);
                _expirationTaskDictionary.TryRemove(key, out expirationTaskWithCts);
            }, cancellationTokenSource.Token);

            expirationTaskWithCts = new Tuple<Task, CancellationTokenSource>(expirationTask, cancellationTokenSource);
            _expirationTaskDictionary.TryAdd(key, expirationTaskWithCts);
            return TaskUtil.CompletedTask;
        }

        public virtual Task SetAbsoluteKeyExpirationAsync(TKey key, DateTimeOffset expiration)
        {
            return SetRelativeKeyExpirationAsync(key, expiration - DateTimeOffset.UtcNow);
        }

        #endregion

        #region IPropertyMap<TKey, TValue> Members

        public virtual Task SetPropertyValueAsync<TProperty>(TKey key, string propertyName, TProperty propertyValue)
        {
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

            return Task.FromResult<object>(null);
        }

        #endregion

        #region IKeysMap<TKey, TValue> Members

        public virtual Task<IAsyncEnumerable<TKey>> GetKeysAsync()
        {
            return Task.FromResult<IAsyncEnumerable<TKey>>(new AsyncEnumerableWrapper<TKey>(InternalDictionary.Keys));
        }

        #endregion

        #region IKeyQueryableMap<TValue> Members

        public virtual Task<QueryResult<TKey>> QueryForKeysAsync<TResult>(Expression<Func<TValue, bool>> @where, Expression<Func<TKey, TResult>> @select, int skip, int take, CancellationToken cancellationToken)
        {
            if (@where == null) @where = value => true;
            if (select != null &&
                select.ReturnType != typeof(TKey))
            {
                throw new NotImplementedException("The select parameter is not supported yet");
            }
            var totalValues = InternalDictionary                
                .Where(pair => where.Compile().Invoke(pair.Value));
            var resultValues = totalValues
                .Skip(skip)
                .Take(take)
                .Select(pair => pair.Key);

            int totalCount = 0;
            if (FetchQueryResultTotal)
            {
                totalCount = totalValues.Count();
            }

            return Task.FromResult(
                new QueryResult<TKey>(new AsyncEnumerableWrapper<TKey>(resultValues), totalCount));
        }

        #endregion

        #region IQueryableStorage<TValue> Members

        public virtual Task<QueryResult<TValue>> QueryAsync<TResult>(Expression<Func<TValue, bool>> @where, Expression<Func<TValue, TResult>> @select, int skip, int take, CancellationToken cancellationToken)
        {
            if (@where == null) @where = value => true;
            if (select != null && 
                select.ReturnType != typeof(TValue))
            {
                throw new NotImplementedException("The select parameter is not supported yet");
            }

            var totalValues = InternalDictionary
                .Where(pair => where.Compile().Invoke(pair.Value));
            var resultValues = totalValues
                .Skip(skip)
                .Take(take)
                .Select(pair => pair.Value);

            int totalCount = 0;
            if (FetchQueryResultTotal)
            {
                totalCount = totalValues.Count();
            }

            return Task.FromResult(
                new QueryResult<TValue>(new AsyncEnumerableWrapper<TValue>(resultValues), totalCount));
        }

        #endregion

        public virtual Task<QueryResult<TValue>> QueryAsync<TResult, TOrderBy>(Expression<Func<TValue, bool>> where, Expression<Func<TValue, TResult>> select, Expression<Func<TValue, TOrderBy>> orderBy, bool orderByAscending, int skip, int take, CancellationToken cancellationToken)
        {
            if (@where == null) @where = value => true;
            if (select != null &&
                select.ReturnType != typeof(TValue))
            {
                throw new NotImplementedException("The select parameter is not supported yet");
            }

            var totalValues = InternalDictionary
                .Values
                .Where(value => where.Compile().Invoke(value));
            var orderByFunc = orderBy.Compile();

            IOrderedEnumerable<TValue> orderedTotalValues;
            if (orderByAscending)
            {
                orderedTotalValues = totalValues.OrderBy(orderByFunc.Invoke);
            }
            else
            {
                orderedTotalValues = totalValues.OrderByDescending(orderByFunc.Invoke);
            }

            var resultValues = orderedTotalValues
                .Skip(skip)
                .Take(take)
                .Select(value => value);

            int totalCount = 0;
            if (FetchQueryResultTotal)
            {
                totalCount = totalValues.Count();
            }


            return Task.FromResult(
                new QueryResult<TValue>(new AsyncEnumerableWrapper<TValue>(resultValues), totalCount));
        }

        protected TValue GetOrCreateValue(TKey key)
        {
            TValue value;

            if (!InternalDictionary.TryGetValue(key, out value))
            {
                value = ValueFactory();
                InternalDictionary.TryAdd(key, value);
            }
            return value;
        }

        public virtual Task<QueryResult<KeyValuePair<TKey, TValue>>> QueryAsync<TResult>(Expression<Func<KeyValuePair<TKey, TValue>, bool>> @where, Expression<Func<KeyValuePair<TKey, TValue>, TResult>> @select, int skip, int take, CancellationToken cancellationToken)
        {
            if (@where == null) @where = value => true;
            if (select != null &&
                select.ReturnType != typeof(KeyValuePair<TKey, TValue>))
            {
                throw new NotImplementedException("The select parameter is not supported yet");
            }
            var totalValues = InternalDictionary
                .Where(pair => where.Compile().Invoke(pair));
            var resultValues = totalValues
                .Skip(skip)
                .Take(take);

            int totalCount = 0;
            if (FetchQueryResultTotal)
            {
                totalCount = totalValues.Count();
            }

            return Task.FromResult(
                new QueryResult<KeyValuePair<TKey, TValue>>(new AsyncEnumerableWrapper<KeyValuePair<TKey, TValue>>(resultValues), totalCount));
        }


    }
}