using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Specialized.Cache
{
    public class OnDemandCacheSet<T> : OnDemandCacheStrategy<ISet<T>>, ISet<T>
    {
        public OnDemandCacheSet(ISet<T> source, ISet<T> cache) : base(source, cache)
        {
        }

        public virtual Task AddAsync(T value, CancellationToken cancellationToken = default) => ExecuteWriteFunc(s => s.AddAsync(value, cancellationToken));

        public virtual Task<bool> TryRemoveAsync(T value, CancellationToken cancellationToken = default) => ExecuteWriteFunc(s => s.TryRemoveAsync(value, cancellationToken));

        public virtual async Task<IAsyncEnumerable<T>> AsEnumerableAsync(CancellationToken cancellationToken = default)
        {
            // The AsEnumerableAsync method always returns a value (never is null)
            // and we are not able to check if it is empty before starting enumerating it,
            // so we will start enumerate it and if there's at least one value, it is valid.

            // TODO: Peek only a value instead of enumerating completely
            var cacheEnumerable = await Cache.AsEnumerableAsync(cancellationToken).ConfigureAwait(false);
            var cacheValues = await cacheEnumerable.ToArrayAsync(cancellationToken).ConfigureAwait(false);

            if (cacheValues.Length > 0)
            {
                return new AsyncEnumerableWrapper<T>(cacheValues);
            }

            var sourceEnumerable = await Source.AsEnumerableAsync(cancellationToken).ConfigureAwait(false);
            var sourceValues = await sourceEnumerable.ToArrayAsync(cancellationToken).ConfigureAwait(false);

            foreach (var sourceValue in sourceValues)
            {
                await Cache.AddAsync(sourceValue, cancellationToken).ConfigureAwait(false);
            }

            return new AsyncEnumerableWrapper<T>(sourceValues);
        }

        public virtual Task<bool> ContainsAsync(T value, CancellationToken cancellationToken = default)
            => ExecuteQueryFunc(
                set => set.ContainsAsync(value, cancellationToken),
                (result, set) =>
                {
                    if (result) return set.AddAsync(value, cancellationToken);
                    return Task.CompletedTask;
                });

        public virtual Task<long> GetLengthAsync(CancellationToken cancellationToken = default)
            => ExecuteQueryFunc(
                set => set.GetLengthAsync(cancellationToken),
                async (result, set) =>
                {
                    if (result > 0)
                    {
                        var enumerable = await Source.AsEnumerableAsync(cancellationToken).ConfigureAwait(false);
                        await enumerable.ForEachAsync(v => set.AddAsync(v, cancellationToken), cancellationToken).ConfigureAwait(false);
                    }
                });

    }
}
