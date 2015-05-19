using System.Collections.Generic;
using System.Threading.Tasks;

namespace Takenet.Elephant.Memory
{
    /// <summary>
    /// Implements the <see cref="ISetMap{TKey,TItem}"/> interface using the <see cref="Map{TKey,TValue}"/> and <see cref="Set{T}"/> classes.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TItem"></typeparam>
    public class SetMap<TKey, TItem> : Map<TKey, ISet<TItem>>, ISetMap<TKey, TItem>, IItemSetMap<TKey, TItem>
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
    }
}
