using System;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Specialized.Synchronization;

namespace Take.Elephant.Specialized.Replication
{
    /// <summary>
    /// Implements a replication mechanism with master and slave actors where when the first fails, it falls back to the second.
    /// When the first actor recovers, it allows the synchronization between them.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class ReplicationStrategy<T> : IDisposable
    {
        private readonly T _master;        
        private readonly T _slave;
        private readonly ISynchronizer<T> _synchronizer;
        private readonly SemaphoreSlim _masterStatusSemaphore;

        private bool _isMasterDown;           

        protected ReplicationStrategy(T master, T slave, ISynchronizer<T> synchronizer)
        {
            if (master == null) throw new ArgumentNullException(nameof(master));
            if (slave == null) throw new ArgumentNullException(nameof(slave));
            if (synchronizer == null) throw new ArgumentNullException(nameof(synchronizer));
            _master = master;
            _slave = slave;
            _synchronizer = synchronizer;
            _masterStatusSemaphore = new SemaphoreSlim(1);
        }

        ~ReplicationStrategy()
        {            
            Dispose(false);
        }

        /// <summary>
        /// Executes an action where if the master fails, the slave take its place. 
        /// </summary>
        /// <param name="func"></param>
        /// <returns></returns>
        protected Task ExecuteWithFallbackAsync(Func<T, Task> func)
        {
            return ExecuteWithFallbackAsync<object>(async arg =>
            {
                await func(arg).ConfigureAwait(false);
                return null;
            });
        }

        /// <summary>
        /// Executes an action where if the master fails, the slave take its place. 
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="func"></param>
        /// <returns></returns>
        protected async Task<TResult> ExecuteWithFallbackAsync<TResult>(Func<T, Task<TResult>> func)
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
        /// Executes an action in both master and slave, even if the first fails.
        /// </summary>
        /// <param name="func"></param>
        /// <returns></returns>
        protected Task ExecuteWithReplicationAsync(Func<T, Task> func)
        {
            return ExecuteWithReplicationAsync<object>(async arg =>
            {
                await func(arg).ConfigureAwait(false);
                return null;
            });
        }

        /// <summary>
        /// Executes an action in both master and slave, even if the first fails.
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="func"></param>
        /// <returns></returns>
        protected async Task<TResult> ExecuteWithReplicationAsync<TResult>(Func<T, Task<TResult>> func)
        {
            var isMasterUp = false;
            TResult value = default(TResult);
            try
            {
                value = await func(_master).ConfigureAwait(false);
                isMasterUp = true;
            }
            catch (Exception ex)
            {
                await RaiseMasterFailedAsync(ex).ConfigureAwait(false);                
            }

            TResult slaveValue = await func(_slave).ConfigureAwait(false);
            if (!isMasterUp) value = slaveValue;
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
                    await _synchronizer.SynchronizeAsync(_slave, _master).ConfigureAwait(false);
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