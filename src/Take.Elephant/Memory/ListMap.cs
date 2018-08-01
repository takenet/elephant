using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Memory
{
    public class ListMap<TKey, TValue> : Map<TKey, IList<TValue>>, IListMap<TKey, TValue>
    {
        public Task<IList<TValue>> GetValueOrEmptyAsync(TKey key)
            => InternalDictionary.GetOrAdd(key, k => ValueFactory()).AsCompletedTask();

        public async Task<bool> TryAddAsync(TKey key, IList<TValue> value, bool overwrite = false)
        {
            var list = ValueFactory();
            var enumerable = await value.AsEnumerableAsync().ConfigureAwait(false);
            await enumerable.ForEachAsync(
                async (i) => await list.AddAsync(i).ConfigureAwait(false), CancellationToken.None)
            .ConfigureAwait(false);
            return await base.TryAddAsync(key, list, overwrite).ConfigureAwait(false);
        }
    }
}
