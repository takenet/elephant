using System;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Tests.Helpers
{
    public static class TestHelpers
    {
        /// <summary>
        /// Repeatedly invokes the supplied async action until the result satisfies the predicate,
        /// the timeout elapses, or the cancellation token is triggered.
        /// </summary>
        public static async Task<T> WaitUntilAsync<T>(
            Func<CancellationToken, Task<T>> action,
            Func<T, bool> isValid,
            TimeSpan timeout,
            TimeSpan retryDelay,
            CancellationToken cancellationToken)
        {
            var startTime = DateTime.UtcNow;

            while (true)
            {
                var result = await action(cancellationToken).ConfigureAwait(false);

                if (isValid(result))
                    return result;

                if (DateTime.UtcNow - startTime >= timeout)
                    return default;

                await Task.Delay(retryDelay, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}

