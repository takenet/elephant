using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Specialized.Cache
{
    public class OnDemandCacheSet<T> : OnDemandCacheBase<ISet<T>>, ISet<T>
    {
        public OnDemandCacheSet(ISet<T> source, ISet<T> cache) 
            : base(source, cache)
        {
        }

        public virtual Task AddAsync(T value, CancellationToken cancellationToken = default) => ExecuteWriteFunc(s => s.AddAsync(value, cancellationToken));

        public virtual Task<bool> TryRemoveAsync(T value, CancellationToken cancellationToken = default) => ExecuteWriteFunc(s => s.TryRemoveAsync(value, cancellationToken));

        public virtual async IAsyncEnumerable<T> AsEnumerableAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            // The AsEnumerableAsync method always returns a value (never is null)
            // and we are not able to check if it is empty before starting enumerating it,
            // so we will start enumerate it and if there's at least one value, it is valid.

            // TODO: Peek only a value instead of enumerating completely
            var cacheEnumerable = Cache.AsEnumerableAsync(cancellationToken);
            var cacheValues = await cacheEnumerable.ToArrayAsync(cancellationToken).ConfigureAwait(false);

            if (cacheValues.Length > 0)
            {
                foreach (var cacheValue in cacheValues)
                {
                    yield return cacheValue;
                }
            }
            else
            {
                var sourceEnumerable = Source.AsEnumerableAsync(cancellationToken);
                var sourceValues = await sourceEnumerable.ToArrayAsync(cancellationToken).ConfigureAwait(false);

                foreach (var sourceValue in sourceValues)
                {
                    await Cache.AddAsync(sourceValue, cancellationToken).ConfigureAwait(false);
                }

                foreach (var sourceValue in sourceValues)
                {
                    yield return sourceValue;
                }
            }
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
                        var enumerable = Source.AsEnumerableAsync(cancellationToken);
                        await enumerable.ForEachAsync(v => set.AddAsync(v, cancellationToken), cancellationToken).ConfigureAwait(false);
                    }
                });

    }
}
