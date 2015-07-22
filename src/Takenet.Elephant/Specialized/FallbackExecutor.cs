using System;
using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized
{
    /// <summary>
    /// Defines a fall back mechanism with a primary and secondary actors. 
    /// For write actions, the operation must succeed in both;
    /// For queries, if the action fails in the first, it falls back to the second.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class FallbackExecutor<T>
    {
        private readonly T _primary;
        private readonly T _secondary;

        protected FallbackExecutor(T primary, T secondary)
        {
            if (primary == null) throw new ArgumentNullException(nameof(primary));
            if (secondary == null) throw new ArgumentNullException(nameof(secondary));
            _primary = primary;
            _secondary = secondary;
        }

        /// <summary>
        /// Executes a write method where both calls must succeed.
        /// </summary>
        /// <param name="func"></param>
        /// <returns></returns>
        protected async Task ExecuteWriteFunc(Func<T, Task> func)
        {
            await func(_primary).ConfigureAwait(false);
            await func(_secondary).ConfigureAwait(false);
        }

        /// <summary>
        /// Executes a write method where if the primary succeeds, the secondary must succeeds to.
        /// </summary>
        /// <param name="func"></param>
        /// <returns></returns>
        protected async Task<bool> ExecuteWriteFunc(Func<T, Task<bool>> func)
        {
            if (!await func(_primary).ConfigureAwait(false)) return false;
            if (!await func(_secondary).ConfigureAwait(false))
            {
                throw new Exception("Could not write to the secondary storage");
            }
            return true;
        }

        /// <summary>
        /// Executes a query method where if the primary fails, the secondary provides the value. 
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
                PrimaryFailed?.Invoke(this, new ExceptionEventArgs(ex));
                value = await func(_secondary).ConfigureAwait(false);
            }

            return value;
        }

        /// <summary>
        /// Occurs when the primary actor throws an exception during a query method.
        /// </summary>
        public event EventHandler<ExceptionEventArgs> PrimaryFailed;
    }
}