using System;
using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized
{
    /// <summary>
    /// Defines a cache mechanism where the write actions are executed against two actors - the source and the cache - 
    /// and the reading ones are executed first in the cache and if not found, in the source. If a value is found in the source,
    /// it is stored in the cache.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class OnDemandCacheStrategy<T>
    {
        protected readonly T Source;
        protected readonly T Cache;

        public OnDemandCacheStrategy(T source, T cache)
        {
            Source = source;
            Cache = cache;
        }

        public async Task<TResult> ExecuteQueryFunc<TResult>(Func<T, Task<TResult>> queryFunc, Func<TResult, T, Task<bool>> writeFunc)
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

        public async Task<bool> ExecuteWriteFunc(Func<T, Task<bool>> func)
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
