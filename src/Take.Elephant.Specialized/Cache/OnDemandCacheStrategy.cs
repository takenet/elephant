using System;
using System.Threading.Tasks;

namespace Take.Elephant.Specialized.Cache
{
    /// <summary>
    /// Defines a cache mechanism where the write actions are executed against two actors - the source and the cache - 
    /// and the reading ones are executed first in the cache and if not found, in the source. If a value is found in the source,
    /// it is stored in the cache.
    /// </summary>
    /// <typeparam name="TMap"></typeparam>
    public class OnDemandCacheStrategy<TMap>
    {
        protected readonly TMap Source;
        protected readonly TMap Cache;

        public OnDemandCacheStrategy(TMap source, TMap cache)
        {
            Source = source ?? throw new ArgumentNullException(nameof(source));
            Cache = cache ?? throw new ArgumentNullException(nameof(cache));
        }

        public virtual Task<TResult> ExecuteQueryFunc<TResult>(Func<TMap, Task<TResult>> queryFunc, Func<TResult, TMap, Task<bool>> writeFunc)
            => ExecuteQueryFunc(queryFunc, async (r, s) => { await writeFunc(r, s); }); // DO NOT SIMPLIFY THIS LAMBDA!

        public virtual async Task<TResult> ExecuteQueryFunc<TResult>(Func<TMap, Task<TResult>> queryFunc, Func<TResult, TMap, Task> writeFunc)
        {
            // Tries in the cache
            var value = await queryFunc(Cache).ConfigureAwait(false);
            if (!IsDefaultValueOfType(value)) return value;

            // Tries in the source
            value = await queryFunc(Source).ConfigureAwait(false);
            if (IsDefaultValueOfType(value)) return value;

            // Caches the value
            await writeFunc(value, Cache).ConfigureAwait(false);
            return value;
        }

        public virtual async Task ExecuteWriteFunc(Func<TMap, Task> func)
        {
            // Writes in the source
            await func(Source).ConfigureAwait(false);
            // Try to write in the cache
            await func(Cache).ConfigureAwait(false);
        }

        public virtual async Task<bool> ExecuteWriteFunc(Func<TMap, Task<bool>> func)
        {
            // Writes in the source
            if (await func(Source).ConfigureAwait(false))
            {
                // Try to write in the cache
                await func(Cache).ConfigureAwait(false);
                // Ignores the cache write result since the source is OK.
                return true;
            }

            return false;
        }

        protected virtual bool IsDefaultValueOfType<TResult>(TResult value) => value.IsDefaultValueOfType(typeof(TResult));
    }
}
