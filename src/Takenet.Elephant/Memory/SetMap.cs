using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace Takenet.Elephant.Memory
{
    /// <summary>
    /// Implements the <see cref="ISetMap{TKey,TItem}"/> interface using the <see cref="Map{TKey,TValue}"/> and <see cref="Set{T}"/> classes.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TItem"></typeparam>
    public class SetMap<TKey, TItem> : Map<TKey, ISet<TItem>>, ISetMap<TKey, TItem>, IItemSetMap<TKey, TItem>, IQueryableStorage<TItem>
    {
        private readonly IEqualityComparer<TItem> _valueEqualityComparer;

        public SetMap()
            : this(EqualityComparer<TItem>.Default)
        {

        }

        public SetMap(IEqualityComparer<TItem> valueEqualityComparer)
            : base(() => new Set<TItem>())
        {
            _valueEqualityComparer = valueEqualityComparer;
        }

        #region IItemSetMap<TKey, TItem> Members

        public async Task<TItem> GetItemOrDefaultAsync(TKey key, TItem item)
        {
            var items = await GetValueOrDefaultAsync(key).ConfigureAwait(false);
            if (items != null)
            {
                return await
                    (await items.AsEnumerableAsync().ConfigureAwait(false))
                        .FirstOrDefaultAsync(i => _valueEqualityComparer.Equals(i, item)).ConfigureAwait(false);
            }

            return default(TItem);
        }

        #endregion

        #region IQueryableStorage<TItem> Members

        public Task<QueryResult<TItem>> QueryAsync<TResult>(Expression<Func<TItem, bool>> @where, Expression<Func<TItem, TResult>> @select, int skip, int take, CancellationToken cancellationToken)
        {
            var items = InternalDictionary
                .Values 
                .Select(v => v.AsEnumerableAsync().Result)                               
                .SelectMany(v => v)
                .Where(where.Compile())
                .Select(select.Compile())
                .Cast<TItem>()
                .Skip(skip)
                .Take(take)
                .ToList();

            return new QueryResult<TItem>(new AsyncEnumerableWrapper<TItem>(items), items.Count).AsCompletedTask();
        }

        #endregion
    }
}
