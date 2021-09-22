using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Memory
{
    /// <summary>
    /// Implements the <see cref="IPartitionedStream{TKey, TEvent}"/> interface using the <see cref="ConcurrentDictionary{TKey, TValue}"/> class.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TEvent"></typeparam>
    public class PartitionedStream<TKey, TEvent> : IPartitionedStream<TKey, TEvent>
    {
        private readonly ConcurrentDictionary<TKey, TEvent> _dictionary;
        private readonly ConcurrentQueue<Tuple<TaskCompletionSource<TEvent>, CancellationTokenRegistration>> _promisesQueue;
        private readonly object _syncRoot = new object();

        public PartitionedStream()
        {
            _dictionary = new ConcurrentDictionary<TKey, TEvent>();
            _promisesQueue = new ConcurrentQueue<Tuple<TaskCompletionSource<TEvent>, CancellationTokenRegistration>>();
        }

        public Task PublishAsync(TKey key, TEvent item, CancellationToken cancellationToken)
        {
            if (item == null)
                throw new ArgumentNullException(nameof(item));
            lock (_syncRoot)
            {
                Tuple<TaskCompletionSource<TEvent>, CancellationTokenRegistration> promise;
                do
                {
                    if (_promisesQueue.TryDequeue(out promise) &&
                        !promise.Item1.Task.IsCanceled &&
                        promise.Item1.TrySetResult(item))
                    {
                        promise.Item2.Dispose();
                        return TaskUtil.CompletedTask;
                    }
                }
                while (promise != null);

                _dictionary.GetOrAdd(key, item);
                return TaskUtil.CompletedTask;
            }
        }
        public async Task<(TKey key, TEvent item)> ConsumeOrDefaultAsync(CancellationToken cancellationToken)
        {
            (TKey, TEvent) result;
            lock (_syncRoot)
            {
                var key = _dictionary.FirstOrDefault().Key;
                result = _dictionary.TryRemove(key, out var eventResult)
                   ? (key, eventResult)
                   : (default(TKey), default(TEvent));
            }
            return await Task.FromResult(result);
        }
    }
}
