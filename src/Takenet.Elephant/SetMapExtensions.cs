using System.Threading.Tasks;
using Takenet.Elephant.Memory;

namespace Takenet.Elephant
{
    public static class SetMapExtensions
    {
        public static async Task AddItemAsync<TKey, TItem>(this ISetMap<TKey, TItem> setMap, TKey key, TItem item)
        {
            ISet<TItem> set = null;
            while ((set = await setMap.GetValueOrDefaultAsync(key).ConfigureAwait(false)) == null)
            {                
                set = new Set<TItem>();
                if (!await setMap.TryAddAsync(key, set).ConfigureAwait(false))
                {
                    set = null;
                }
            }

            await set.AddAsync(item).ConfigureAwait(false);
        }
    }
}