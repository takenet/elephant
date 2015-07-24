using System;
using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized
{
    /// <summary>
    /// Defines a fall back mechanism with primary and backup actors. 
    /// For write actions, the operation must succeed in both;
    /// For queries, if the action fails in the first, it falls back to the second.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class BackupStrategy<T>
    {
        private readonly T _primary;
        private readonly T _backup;

        protected BackupStrategy(T primary, T backup)
        {
            if (primary == null) throw new ArgumentNullException(nameof(primary));
            if (backup == null) throw new ArgumentNullException(nameof(backup));
            _primary = primary;
            _backup = backup;
        }

        /// <summary>
        /// Executes a write method where both calls must succeed.
        /// </summary>
        /// <param name="func"></param>
        /// <returns></returns>
        protected async Task ExecuteWriteFunc(Func<T, Task> func)
        {
            await func(_primary).ConfigureAwait(false);
            await func(_backup).ConfigureAwait(false);
        }

        /// <summary>
        /// Executes a write method where if the primary succeeds, the backup must succeeds to.
        /// </summary>
        /// <param name="func"></param>
        /// <returns></returns>
        protected async Task<bool> ExecuteWriteFunc(Func<T, Task<bool>> func)
        {
            if (!await func(_primary).ConfigureAwait(false)) return false;
            if (!await func(_backup).ConfigureAwait(false))
            {
                throw new Exception("Could not write to the backup storage");
            }
            return true;
        }

        /// <summary>
        /// Executes a query method where if the primary fails, the backup provides the value. 
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="func"></param>
        /// <returns></returns>
        protected async Task<TResult> ExecuteQueryFunc<TResult>(Func<T, Task<TResult>> func)
        {
            TResult value;
            try
            {
                value = await func(_primary).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                await RaisePrimaryFailedAsync(ex).ConfigureAwait(false);
                value = await func(_backup).ConfigureAwait(false);
            }

            return value;
        }

        /// <summary>
        /// Occurs when the primary actor throws an exception during a query method.
        /// </summary>
        public event EventHandler<ExceptionEventArgs> PrimaryFailed;

        /// <summary>
        /// Occurs when the master actor throws an exception during a query method.
        /// </summary>
        /// <param name="exception"></param>
        /// <returns></returns>
        protected virtual Task OnPrimaryFailedAsync(Exception exception)
        {
            return TaskUtil.CompletedTask;
        }

        private async Task RaisePrimaryFailedAsync(Exception exception)
        {
            await OnPrimaryFailedAsync(exception).ConfigureAwait(false);
            PrimaryFailed?.Invoke(this, new ExceptionEventArgs(exception));
        }
    }
}