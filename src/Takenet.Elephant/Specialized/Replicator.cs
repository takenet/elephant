using System;
using System.Threading;
using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized
{
    /// <summary>
    /// Implements a replication mechanism with master and slave actors where when the first fails, it falls back to the second.
    /// When the first actor recovers, it allows the synchronization between they.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class Replicator<T> : IDisposable
    {
        private readonly T _master;        
        private readonly T _slave;
        private readonly ISynchronizer<T> _synchronizer;
        private readonly SemaphoreSlim _masterStatusSemaphore;

        private bool _isMasterDown;           

        protected Replicator(T master, T slave, ISynchronizer<T> synchronizer)
        {
            if (master == null) throw new ArgumentNullException(nameof(master));
            if (slave == null) throw new ArgumentNullException(nameof(slave));
            if (synchronizer == null) throw new ArgumentNullException(nameof(synchronizer));
            _master = master;
            _slave = slave;
            _synchronizer = synchronizer;
            _masterStatusSemaphore = new SemaphoreSlim(1);
        }

        ~Replicator()
        {            
            Dispose(false);
        }

        /// <summary>
        /// Executes a query method where if the master fails, the slave take its place. 
        /// </summary>
        /// <param name="func"></param>
        /// <returns></returns>
        protected Task ExecuteAsync(Func<T, Task> func)
        {
            return ExecuteAsync<object>(async arg =>
            {
                await func(arg).ConfigureAwait(false);
                return null;
            });
        }

        /// <summary>
        /// Executes a query method where if the master fails, the slave take its place. 
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="func"></param>
        /// <returns></returns>
        protected async Task<TResult> ExecuteAsync<TResult>(Func<T, Task<TResult>> func)
        {
            var isMasterUp = false;
            TResult value;
            try
            {
                value = await func(_master).ConfigureAwait(false);
                isMasterUp = true;
            }
            catch (Exception ex)
            {
                await RaiseMasterFailedAsync(ex).ConfigureAwait(false);
                value = await func(_slave).ConfigureAwait(false);
            }

            if (isMasterUp && _isMasterDown) await CheckSynchronizationAsync().ConfigureAwait(false);
            return value;
        }

        /// <summary>
        /// Occurs when the master actor throws an exception during a call.
        /// </summary>
        public event EventHandler<ExceptionEventArgs> MasterFailed;

        /// <summary>
        /// Occurs when the master actor change its status to down.
        /// </summary>
        public event EventHandler MasterDown;

        /// <summary>
        /// Occurs when the master actor change its status to up after being down.
        /// </summary>
        public event EventHandler MasterRecovered;

        /// <summary>
        /// Occurs when the master actor throws an exception during a query method.
        /// </summary>
        /// <param name="exception"></param>
        /// <returns></returns>
        protected virtual Task OnMasterFailedAsync(Exception exception)
        {
            return TaskUtil.CompletedTask;
        }

        /// <summary>
        /// Occurs when the master actor change its status to down
        /// </summary>
        /// <returns></returns>
        protected virtual Task OnMasterDownAsync()
        {
            return TaskUtil.CompletedTask;
        }

        /// <summary>
        /// Occurs when the master actor change its status to up after being down.
        /// </summary>
        /// <returns></returns>
        protected virtual Task OnMasterRecoveredAsync()
        {
            return TaskUtil.CompletedTask;
        }

        private async Task RaiseMasterFailedAsync(Exception exception)
        {
            await OnMasterFailedAsync(exception).ConfigureAwait(false);
            MasterFailed?.Invoke(this, new ExceptionEventArgs(exception));

            if (!_isMasterDown)
            {
                await _masterStatusSemaphore.WaitAsync().ConfigureAwait(false);
                try
                {
                    if (!_isMasterDown)
                    {
                        _isMasterDown = true;
                        await RaiseMasterDownAsync().ConfigureAwait(false);
                    }
                }
                finally
                {
                    _masterStatusSemaphore.Release();
                }
            }
        }

        private async Task RaiseMasterDownAsync()
        {
            await OnMasterDownAsync().ConfigureAwait(false);
            MasterDown?.Invoke(this, EventArgs.Empty);
        }

        private async Task RaiseMasterRecoveredAsync()
        {
            await OnMasterRecoveredAsync().ConfigureAwait(false);
            MasterRecovered?.Invoke(this, EventArgs.Empty);
        }
        private async Task CheckSynchronizationAsync()
        {
            await _masterStatusSemaphore.WaitAsync().ConfigureAwait(false);
            try
            {
                if (_isMasterDown)
                {
                    await _synchronizer.SynchronizeAsync(_master, _slave).ConfigureAwait(false);
                    _isMasterDown = false;
                    await RaiseMasterRecoveredAsync().ConfigureAwait(false);
                }
            }
            finally
            {
                _masterStatusSemaphore.Release();
            }
        }

        public virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _masterStatusSemaphore.Dispose();
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}