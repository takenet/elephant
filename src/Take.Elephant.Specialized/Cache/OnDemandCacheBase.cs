using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Take.Elephant.Specialized.Cache
{
    /// <summary>
    /// Defines a cache mechanism where the write actions are executed against two actors - the source and the cache - 
    /// and the reading ones are executed first in the cache and if not found, in the source. If a value is found in the source,
    /// it is stored in the cache.
    /// </summary>
    /// <typeparam name="TMap"></typeparam>
    public class OnDemandCacheBase<TMap>
    {
        public OnDemandCacheBase(TMap source, TMap cache)
            : this(source, cache, new CacheOptions(), new TraceLogger())
        {
        }

        public OnDemandCacheBase(TMap source, TMap cache, CacheOptions cacheOptions, ILogger logger)
        {
            Source = source ?? throw new ArgumentNullException(nameof(source));
            Cache = cache ?? throw new ArgumentNullException(nameof(cache));
            CacheOptions = cacheOptions ?? throw new ArgumentNullException(nameof(cacheOptions));
            Logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        protected TMap Source { get; }

        protected TMap Cache { get; }

        protected CacheOptions CacheOptions { get; }

        protected ILogger Logger { get; }

        public virtual async Task<TResult> ExecuteQueryFunc<TResult>(Func<TMap, Task<TResult>> queryFunc, Func<TResult, TMap, Task<bool>> writeFunc)
            => await ExecuteQueryFunc(queryFunc, (r, s) => (Task)writeFunc(r, s));

        public virtual async Task<TResult> ExecuteQueryFunc<TResult>(Func<TMap, Task<TResult>> queryFunc, Func<TResult, TMap, Task> writeFunc)
        {
            // Tries in the cache
            var value = await queryFunc(Cache).ConfigureAwait(false);
            if (!IsDefaultValueOfType(value))
            {
                return value;
            }

            // Tries in the source
            value = await queryFunc(Source).ConfigureAwait(false);
            if (IsDefaultValueOfType(value))
            {
                return value;
            }

            // Caches the value
            try
            {
                await writeFunc(value, Cache).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                if (CacheOptions.ThrowOnCacheWritingExceptions)
                {
                    throw;
                }

                Logger.LogWarning(e, "Failed to write retrieved value from source to cache (this exception is being ignored and won't affect the read from source)");
            }
            return value;
        }

        public virtual async Task ExecuteWriteFunc(Func<TMap, Task> func)
        {
            // Writes in the source
            await func(Source).ConfigureAwait(false);

            // Try to write in the cache
            try
            {
                await func(Cache).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                if (CacheOptions.ThrowOnCacheWritingExceptions)
                {
                    throw;
                }

                Logger.LogWarning(e, "Failed to write to cache (this exception is being ignored and won't affect the write to source)");
            }
        }

        public virtual async Task<bool> ExecuteWriteFunc(Func<TMap, Task<bool>> func)
        {
            // Writes in the source
            if (await func(Source).ConfigureAwait(false))
            {
                try
                {
                    // Try to write in the cache
                    await func(Cache).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    if (CacheOptions.ThrowOnCacheWritingExceptions)
                    {
                        throw;
                    }

                    Logger.LogWarning(e, "Failed to write to cache (this exception is being ignored and won't affect the write to source)");
                }

                // Ignores the cache write result since the source is OK.
                return true;

            }

            return false;
        }

        protected virtual bool IsDefaultValueOfType<TResult>(TResult value) => value.IsDefaultValueOfType(typeof(TResult));
    }
}
