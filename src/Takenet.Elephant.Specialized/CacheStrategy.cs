using System;
using System.Threading;
using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized
{
    /// <summary>
    /// Defines a cache mechanism where the write actions are executed against two actors - the source and the cache - but the reading ones only against the cache.
    /// In case of failure of writing in the cache actor, there's a chance of synchronization between the actors before the next read.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class CacheStrategy<T> : IDisposable
    {
        protected readonly T Source;
        protected readonly T Cache;

        private readonly SemaphoreSlim _writeSemaphore;
        private readonly ISynchronizer<T> _synchronizer;
        private readonly TimeSpan _cacheExpiration;

        private bool _isSynchronized;
        private DateTimeOffset _lastSynchronization;        

        /// <summary>
        /// Initialize the instance.
        /// </summary>
        /// <param name="source">The source storage</param>
        /// <param name="cache">The cache storage, which all queries will be executed against.</param>
        /// <param name="synchronizer">The synchronizer which will be executed on the first time that a query operation is called.</param>
        /// <param name="cacheExpiration">The period to invalidate the cache, forcing a new synchronization to be executed.</param>
        protected CacheStrategy(T source, T cache, ISynchronizer<T> synchronizer, TimeSpan cacheExpiration = default(TimeSpan))
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (cache == null) throw new ArgumentNullException(nameof(cache));            
            if (synchronizer == null) throw new ArgumentNullException(nameof(synchronizer));
            Source = source;
            Cache = cache;
            _synchronizer = synchronizer;
            _cacheExpiration = cacheExpiration;
            _writeSemaphore = new SemaphoreSlim(1);            
        }

        ~CacheStrategy()
        {
            Dispose(false);
        }

        /// <summary>
        /// Occurs when the cache actor throws an exception during a write method.
        /// </summary>
        public event EventHandler<ExceptionEventArgs> CacheFailed;

        public virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _writeSemaphore.Dispose();
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }


        protected bool IsSynchronized
        {
            get
            {
                if (_isSynchronized)
                {
                    if (_cacheExpiration.Equals(default(TimeSpan))) return true;
                    return (DateTimeOffset.UtcNow - _lastSynchronization) < _cacheExpiration;
                }

                return false;
            }
            set
            {
                _isSynchronized = value;
            }
        }

        /// <summary>
        /// Executes a write method where both calls must succeed.
        /// </summary>
        /// <param name="func"></param>
        /// <returns></returns>
        protected virtual async Task ExecuteWriteFunc(Func<T, Task> func)
        {
            if (!IsSynchronized) await Synchronize();

            await func(Source).ConfigureAwait(false);
            try
            {
                await func(Cache).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                await RaiseCacheFailedAsync(ex).ConfigureAwait(false);
                IsSynchronized = false;
            }

        }

        /// <summary>
        /// Executes a write method where both calls must succeed.
        /// </summary>
        /// <param name="func"></param>
        /// <returns></returns>
        protected async Task<bool> ExecuteWriteFunc(Func<T, Task<bool>> func)
        {
            if (!IsSynchronized) await Synchronize();

            if (!await func(Source).ConfigureAwait(false)) return false;
            try
            {
                if (!await func(Cache).ConfigureAwait(false))
                {
                    IsSynchronized = false;
                }
            }
            catch (Exception ex)
            {
                await RaiseCacheFailedAsync(ex).ConfigureAwait(false);
                IsSynchronized = false;
            }
            return true;

        }

        /// <summary>
        /// Executes a query method where queries only in the cache. 
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="func"></param>
        /// <returns></returns>
        protected virtual async Task<TResult> ExecuteQueryFunc<TResult>(Func<T, Task<TResult>> func)
        {
            if (!IsSynchronized) await Synchronize();            

            return await func(Cache).ConfigureAwait(false);
        }

        /// <summary>
        /// Occurs when the cache actor throws an exception during a write method.
        /// </summary>
        /// <param name="exception"></param>
        /// <returns></returns>
        protected virtual Task OnCacheFailedAsync(Exception exception)
        {
            return TaskUtil.CompletedTask;
        }

        private async Task RaiseCacheFailedAsync(Exception exception)
        {
            await OnCacheFailedAsync(exception).ConfigureAwait(false);
            CacheFailed?.Invoke(this, new ExceptionEventArgs(exception));
        }

        private async Task Synchronize()
        {
            await _writeSemaphore.WaitAsync();
            try
            {
                if (!IsSynchronized)
                {
                    await _synchronizer.SynchronizeAsync(Source, Cache).ConfigureAwait(false);
                    IsSynchronized = true;
                    _lastSynchronization = DateTimeOffset.UtcNow;
                }
            }
            finally
            {
                _writeSemaphore.Release();
            }
        }

    }
}
