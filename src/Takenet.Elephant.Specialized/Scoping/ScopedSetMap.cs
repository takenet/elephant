using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized.Scoping
{
    public class ScopedSetMap<TKey, TItem> : ScopedMap<TKey, ISet<TItem>>, ISetMap<TKey, TItem>, IItemSetMap<TKey, TItem>, IQueryableStorage<TItem>, IQueryableStorage<KeyValuePair<TKey, TItem>>, IKeyQueryableMap<TKey, TItem>
    {
        public ScopedSetMap(IMap<TKey, ISet<TItem>> map, IScope scope, ISerializer<TKey> keySerializer) 
            : base(map, scope, keySerializer)
        {
        }

        public virtual Task<TItem> GetItemOrDefaultAsync(TKey key, TItem item) =>
            CastMapOrThrow<IItemSetMap<TKey, TItem>>().GetItemOrDefaultAsync(key, item);

        public virtual Task<QueryResult<TItem>> QueryAsync<TResult>(Expression<Func<TItem, bool>> @where, Expression<Func<TItem, TResult>> @select, int skip, int take, CancellationToken cancellationToken) =>
            CastMapOrThrow<IQueryableStorage<TItem>>().QueryAsync(@where, @select, skip, take, cancellationToken);

        public virtual Task<QueryResult<KeyValuePair<TKey, TItem>>> QueryAsync<TResult>(Expression<Func<KeyValuePair<TKey, TItem>, bool>> @where, Expression<Func<KeyValuePair<TKey, TItem>, TResult>> @select, int skip, int take, CancellationToken cancellationToken) =>
            CastMapOrThrow<IQueryableStorage<KeyValuePair<TKey, TItem>>>().QueryAsync(@where, @select, skip, take, cancellationToken);

        public virtual Task<QueryResult<TKey>> QueryForKeysAsync<TResult>(Expression<Func<TItem, bool>> @where, Expression<Func<TKey, TResult>> @select, int skip, int take, CancellationToken cancellationToken) =>
            CastMapOrThrow<IKeyQueryableMap<TKey, TItem>>().QueryForKeysAsync(@where, @select, skip, take, cancellationToken);
    }
}