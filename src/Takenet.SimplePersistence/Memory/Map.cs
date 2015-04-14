using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence.Memory
{
    /// <summary>
    /// Implemens the <see cref="IMap{TKey,TValue}"/> interface using the <see cref="System.Collections.Concurrent.ConcurrentDictionary{TKey,TValue}"/> class.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public class Map<TKey, TValue> : IUpdatableMap<TKey, TValue>, IExpirableKeyMap<TKey, TValue>, IPropertyMap<TKey, TValue> 
         //, IQueryableStorage<TValue>
    {
        public Map()
            : this(() => (TValue)Activator.CreateInstance(typeof(TValue)))
        {

        }

        public Map(Func<TValue> valueFactory)
            : this(valueFactory, new TypeDictionaryConverter<TValue>(valueFactory))
        {
         
        }

        public Map(Func<TValue> valueFactory, IDictionaryConverter<TValue> dictionaryConverter)
        {
            ValueFactory = valueFactory;
            DictionaryConverter = dictionaryConverter;
            InternalDictionary = new ConcurrentDictionary<TKey, TValue>();
        }

        protected ConcurrentDictionary<TKey, TValue> InternalDictionary { get; }
        protected Func<TValue> ValueFactory { get; }
        protected IDictionaryConverter<TValue> DictionaryConverter { get; }

        #region IMap<TKey,TValue> Members

        public Task<bool> TryAddAsync(TKey key, TValue value, bool overwrite = false)
        {
            if (overwrite)
            {
                InternalDictionary.AddOrUpdate(key, value, (k, v) => value);
                return Task.FromResult(true);
            }

            return Task.FromResult(InternalDictionary.TryAdd(key, value));
        }

        public Task<TValue> GetValueOrDefaultAsync(TKey key)
        {
            TValue value;
            return Task.FromResult(InternalDictionary.TryGetValue(key, out value) ? value : default(TValue));
        }

        public Task<bool> TryRemoveAsync(TKey key)
        {
            TValue value;
            return Task.FromResult(InternalDictionary.TryRemove(key, out value));
        }

        public Task<bool> ContainsKeyAsync(TKey key)
        {
            return Task.FromResult(InternalDictionary.ContainsKey(key));
        }

        #endregion

        #region IUpdatableMap<TKey,TValue> Members

        public Task<bool> TryUpdateAsync(TKey key, TValue newValue, TValue oldValue)
        {
            return InternalDictionary.TryUpdate(key, newValue, oldValue).AsCompletedTask();
        }

        #endregion

        #region IExpirableKeyMap<TKey,TValue> Members

        private readonly ConcurrentDictionary<TKey, Tuple<Task, CancellationTokenSource>> _expirationTaskDictionary = new ConcurrentDictionary<TKey, Tuple<Task, CancellationTokenSource>>();

        public Task SetRelativeKeyExpirationAsync(TKey key, TimeSpan ttl)
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

        public Task SetAbsoluteKeyExpirationAsync(TKey key, DateTimeOffset expiration)
        {
            return SetRelativeKeyExpirationAsync(key, expiration - DateTimeOffset.UtcNow);
        }

        #endregion

        #region IPropertyMap<TKey, TValue> Members

        public Task SetPropertyValueAsync<TProperty>(TKey key, string propertyName, TProperty propertyValue)
        {
            TValue value = GetOrCreateValue(key);
            var property = typeof(TValue).GetProperty(
                propertyName,
                BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);

            if (property == null) throw new ArgumentException("The property name is invalid", nameof(propertyName));
            property.SetValue(value, propertyValue);

            return Task.FromResult<object>(null);
        }

        public Task<TProperty> GetPropertyValueOrDefaultAsync<TProperty>(TKey key, string propertyName)
        {
            TValue value;

            if (!InternalDictionary.TryGetValue(key, out value))
                throw new ArgumentException("The property name is invalid", nameof(propertyName));

            var property = typeof(TValue).GetProperty(
                propertyName,
                BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);

            TProperty propertyValue = default(TProperty);
            if (property != null) propertyValue = (TProperty)property.GetValue(value);

            return Task.FromResult(propertyValue);
        }

        public Task MergeAsync(TKey key, TValue value)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (value == null) throw new ArgumentNullException(nameof(value));

            var properties = DictionaryConverter.ToDictionary(value);
            var existingValue = GetOrCreateValue(key);

            foreach (var propertyKeyValue in properties.Where(p => p.Value != null))
            {
                var property = typeof(TValue)
                    .GetProperty(
                        propertyKeyValue.Key,
                        BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);

                if (property != null)
                {
                    try
                    {
                        if (property.PropertyType.IsEnum)
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
    }
}