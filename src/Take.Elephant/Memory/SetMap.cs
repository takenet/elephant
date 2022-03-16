using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Memory
{
    /// <summary>
    /// Implements the <see cref="ISetMap{TKey,TItem}"/> interface using the <see cref="Map{TKey,TValue}"/> and <see cref="Set{T}"/> classes.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TItem"></typeparam>
    public class SetMap<TKey, TItem> : Map<TKey, ISet<TItem>>, ISetMap<TKey, TItem>, IItemSetMap<TKey, TItem>, IQueryableStorage<TItem>, IQueryableStorage<KeyValuePair<TKey, TItem>>, IKeyQueryableMap<TKey, TItem>
    {
        private readonly IEqualityComparer<TItem> _valueEqualityComparer;

        public SetMap(TimeSpan expirationScanInterval = default)
            : this(EqualityComparer<TItem>.Default, expirationScanInterval)
        {
        }

        public SetMap(IEqualityComparer<TItem> valueEqualityComparer, TimeSpan expirationScanInterval = default)
            : base(() => new Set<TItem>(valueEqualityComparer), expirationScanInterval)
        {
            _valueEqualityComparer = valueEqualityComparer;
        }
        public bool SupportsEmptySets => false;

        public override async Task<bool> TryAddAsync(
            TKey key,
            ISet<TItem> value,
            bool overwrite = false,
            CancellationToken cancellationToken = default)
        {
            if (!(value is Set<TItem> set))
            {
                set = new Set<TItem>();
                await value
                    .AsEnumerableAsync(cancellationToken)
                    .ForEachAwaitAsync(i => set.AddAsync(i, cancellationToken), cancellationToken)
                    .ConfigureAwait(false);
            }

            return await base.TryAddAsync(key, set, overwrite, cancellationToken).ConfigureAwait(false);
        }

        public virtual async Task<TItem> GetItemOrDefaultAsync(TKey key, TItem item)
        {
            var items = await GetValueOrDefaultAsync(key).ConfigureAwait(false);
            if (items != null)
            {
                return await items
                    .AsEnumerableAsync()
                    .FirstOrDefaultAsync(i => _valueEqualityComparer.Equals(i, item))
                    .ConfigureAwait(false);
            }

            return default;
        }

        public virtual Task<ISet<TItem>> GetValueOrEmptyAsync(TKey key, CancellationToken cancellationToken = default) => 
            Task.FromResult(InternalDictionary.GetOrAdd(key, k => ValueFactory()));

        public virtual Task<QueryResult<TItem>> QueryAsync<TResult>(
            Expression<Func<TItem, bool>> @where,
            Expression<Func<TItem, TResult>> @select,
            int skip,
            int take,
            CancellationToken cancellationToken)
        {
            if (@where == null) @where = value => true;
            if (select != null &&
                select.ReturnType != typeof(TItem))
            {
                throw new NotImplementedException("The select parameter is not supported yet");
            }
            
            var totalValues = InternalDictionary
                .Values
                .Select(v => ((Set<TItem>)v).HashSet)
                .SelectMany(v => v)
                .Where(where.Compile());
            
            var totalCount = 0;
            if (FetchQueryResultTotal)
            {
                totalCount = totalValues.Count();
            }

            var resultValues = totalValues
                .Skip(skip)
                .Take(take)
                .Select(pair => pair);

            var queryResult = new QueryResult<TItem>(resultValues, totalCount);
            return Task.FromResult(queryResult);
        }

        public virtual Task<QueryResult<KeyValuePair<TKey, TItem>>> QueryAsync<TResult>(Expression<Func<KeyValuePair<TKey, TItem>, bool>> @where, Expression<Func<KeyValuePair<TKey, TItem>, TResult>> @select, int skip, int take, CancellationToken cancellationToken)
        {
            if (@where == null) @where = value => true;
            if (select != null &&
                select.ReturnType != typeof(KeyValuePair<TKey, TItem>))
            {
                throw new NotImplementedException("The select parameter is not supported yet");
            }
            var totalValues = InternalDictionary
                .SelectMany(v => v
                    .Value
                    .ToListAsync().Result
                    .Select(i => new KeyValuePair<TKey, TItem>(v.Key, i)))
                .Where(pair => where.Compile().Invoke(pair));
            
            var totalCount = 0;
            if (FetchQueryResultTotal)
            {
                totalCount = totalValues.Count();
            }

            var resultValues = totalValues
                .Skip(skip)
                .Take(take);

            var queryResult = new QueryResult<KeyValuePair<TKey, TItem>>(resultValues, totalCount);
            return Task.FromResult(queryResult);
        }

        public virtual Task<QueryResult<TKey>> QueryForKeysAsync<TResult>(Expression<Func<TItem, bool>> @where, Expression<Func<TKey, TResult>> @select, int skip, int take,
            CancellationToken cancellationToken)
        {
            if (@where == null) @where = value => true;
            if (select != null &&
                select.ReturnType != typeof(TKey))
            {
                throw new NotImplementedException("The select parameter is not supported yet");
            }

            var predicate = where.Compile();
            var totalValues = this.InternalDictionary
                .Where(pair =>
                    pair.Value.ToListAsync().Result.Any(m =>
                        predicate.Invoke(m)));
            
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
    }
}