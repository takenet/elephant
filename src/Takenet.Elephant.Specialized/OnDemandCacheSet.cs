using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized
{
    public class OnDemandCacheSet<T> : OnDemandCacheStrategy<ISet<T>>, ISet<T>
    {
        public OnDemandCacheSet(ISet<T> source, ISet<T> cache) : base(source, cache)
        {
        }

        public Task AddAsync(T value) => ExecuteWriteFunc(s => s.AddAsync(value));

        public Task<bool> TryRemoveAsync(T value) => ExecuteWriteFunc(s => s.TryRemoveAsync(value));

        public async Task<IAsyncEnumerable<T>> AsEnumerableAsync()
        {
            // The AsEnumerableAsync method always returns a value (never is null)
            // and we are not able to check if it is empty before starting enumerating it,
            // so we will start enumerate it and if there's at least one value, it is valid.

            // TODO: Peek only a value instead of enumerating completely
            var cacheEnumerable = await Cache.AsEnumerableAsync().ConfigureAwait(false);
            var cacheValues = await cacheEnumerable.ToArrayAsync().ConfigureAwait(false);

            if (cacheValues.Length > 0)
            {
                return new AsyncEnumerableWrapper<T>(cacheValues);
            }

            var sourceEnumerable = await Source.AsEnumerableAsync().ConfigureAwait(false);
            var sourceValues = await sourceEnumerable.ToArrayAsync().ConfigureAwait(false);

            foreach (var sourceValue in sourceValues)
            {
                await Cache.AddAsync(sourceValue).ConfigureAwait(false);
            }

            return new AsyncEnumerableWrapper<T>(sourceValues);
        }

        public Task<bool> ContainsAsync(T value)
            => ExecuteQueryFunc(
                set => set.ContainsAsync(value),
                (result, set) =>
                {
                    if (result) return set.AddAsync(value);
                    return Task.CompletedTask;
                });

        public Task<long> GetLengthAsync()
            => ExecuteQueryFunc(
                set => set.GetLengthAsync(),
                async (result, set) =>
                {
                    if (result > 0)
                    {
                        var enumerable = await Source.AsEnumerableAsync().ConfigureAwait(false);
                        await enumerable.ForEachAsync(v => set.AddAsync(v)).ConfigureAwait(false);
                    }
                });

    }
}
