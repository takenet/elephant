using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence.Memory
{
    /// <summary>
    /// Implemens the IMap interface  using a concurrent dictionary. 
    /// This class should be used only for local data, since the dictionary stores the values in the local process memory.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public class DictionaryMap<TKey, TValue> : IMap<TKey, TValue>, IQueryableStorage<TValue>, IUpdatableMap<TKey, TValue>, IExpirableKeyMap<TKey, TValue>
    {
        protected readonly ConcurrentDictionary<TKey, TValue> _internalDictionary;
        protected readonly Func<TValue> _valueFactory;

        public DictionaryMap()
            : this(() => (TValue)Activator.CreateInstance(typeof(TValue)))
        {

        }

        public DictionaryMap(Func<TValue> valueFactory)
        {
            _valueFactory = valueFactory;

            _internalDictionary = new ConcurrentDictionary<TKey, TValue>();
        }

        #region IMap<TKey,TValue> Members

        public Task<bool> TryAddAsync(TKey key, TValue value, bool overwrite = false)
        {
            if (overwrite)
            {
                _internalDictionary.AddOrUpdate(key, value, (k, v) => value);
                return Task.FromResult(true);
            }

            return Task.FromResult(_internalDictionary.TryAdd(key, value));
        }

        public Task<TValue> GetValueOrDefaultAsync(TKey key)
        {
            TValue value;

            if (_internalDictionary.TryGetValue(key, out value))
            {
                return Task.FromResult(value);
            }

            return Task.FromResult(default(TValue));
        }

        public Task<bool> TryRemoveAsync(TKey key)
        {
            TValue value;
            return Task.FromResult(_internalDictionary.TryRemove(key, out value));
        }


        public Task<bool> ContainsKeyAsync(TKey key)
        {
            return Task.FromResult(_internalDictionary.ContainsKey(key));
        }

        #endregion

        #region IQueryableStorage<TValue>

        public Task<QueryResult<TValue>> QueryAsync<TResult>(Expression<Func<TValue, bool>> where,
            Expression<Func<TValue, TResult>> select,
            int skip,
            int take,
            CancellationToken cancellationToken)
        {
            var predicate = where.Compile();

            var totalValues = this._internalDictionary
                .Where(pair => predicate.Invoke(pair.Value));

            var resultValues = totalValues
                .Skip(skip)
                .Take(take)
                .Select(pair => pair.Value)
                .ToArray();

            var result = new QueryResult<TValue>(resultValues, totalValues.Count());

            return Task.FromResult(result);
        }

        #endregion

        protected TValue GetOrCreateValue(TKey key)
        {
            TValue value;

            if (!_internalDictionary.TryGetValue(key, out value))
            {
                value = _valueFactory();
                _internalDictionary.TryAdd(key, value);
            }
            return value;
        }

        #region IUpdatableMap<TKey,TValue> Members

        public Task<bool> TryUpdateAsync(TKey key, TValue newValue, TValue oldValue)
        {
            return _internalDictionary.TryUpdate(key, newValue, oldValue).AsCompletedTask();
        }

        #endregion

        #region IExpirableKeyMap<TKey,TValue> Members



        private readonly ConcurrentDictionary<TKey, Tuple<Task, CancellationTokenSource>> _expirationTaskDictionary = new ConcurrentDictionary<TKey, Tuple<Task, CancellationTokenSource>>();


        public Task SetRelativeKeyExpirationAsync(TKey key, TimeSpan ttl)
        {
            Tuple<Task, CancellationTokenSource> expirationTaskWithCancellation;
            if (_expirationTaskDictionary.TryRemove(key, out expirationTaskWithCancellation))
            {
                expirationTaskWithCancellation.Item2.Cancel();
            }

            var cancellationTokenSource = new CancellationTokenSource();
            var expirationTask = Task.Run(async () =>
            {
                await Task.Delay(ttl, cancellationTokenSource.Token);
                await TryRemoveAsync(key);
                _expirationTaskDictionary.TryRemove(key, out expirationTaskWithCancellation);
            }, cancellationTokenSource.Token);

            expirationTaskWithCancellation = new Tuple<Task, CancellationTokenSource>(expirationTask, cancellationTokenSource);
            _expirationTaskDictionary.TryAdd(key, expirationTaskWithCancellation);
            return TaskUtil.CompletedTask;
        }

        public Task SetAbsoluteKeyExpirationAsync(TKey key, DateTimeOffset expiration)
        {
            return SetRelativeKeyExpirationAsync(key, expiration - DateTimeOffset.UtcNow);
        }

        #endregion
    }
}
