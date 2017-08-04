using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Takenet.Elephant.Memory
{
    /// <summary>
    /// Implements the <see cref="IQueue{T}"/> interface using the <see cref="System.Collections.Concurrent.ConcurrentQueue{T}"/> class.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class Queue<T> : IBlockingQueue<T>, ICloneable
    {
        private readonly ConcurrentQueue<T> _queue;
        private readonly ConcurrentQueue<Tuple<TaskCompletionSource<T>, CancellationTokenRegistration>> _promisesQueue;
        private readonly object _syncRoot = new object();

        public Queue()
        {            
            _queue = new ConcurrentQueue<T>();
            _promisesQueue = new ConcurrentQueue<Tuple<TaskCompletionSource<T>, CancellationTokenRegistration>>();
        }

        public Task EnqueueAsync(T item)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));
            lock (_syncRoot)
            {
                Tuple<TaskCompletionSource<T>, CancellationTokenRegistration> promise;
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

                _queue.Enqueue(item);
                return TaskUtil.CompletedTask;
            }
        }

        public Task<T> DequeueOrDefaultAsync()
        {
            lock (_syncRoot)
            {
                T result;
                return _queue.TryDequeue(out result)
                    ? result.AsCompletedTask()
                    : default(T).AsCompletedTask();
            }
        }

        public Task<long> GetLengthAsync()
        {
            return ((long)_queue.Count).AsCompletedTask();
        }

        public Task<T> DequeueAsync(CancellationToken cancellationToken)
        {
            T item;
            if (!_queue.TryDequeue(out item))
            {
                lock (_syncRoot)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (!_queue.TryDequeue(out item))
                    {
                        var promise = new TaskCompletionSource<T>();
                        var registration = cancellationToken.Register(() => promise.TrySetCanceled());
                        _promisesQueue.Enqueue(Tuple.Create(promise, registration));
                        return promise.Task;
                    }
                }
            }

            return item.AsCompletedTask();
        }

        /// <summary>
        /// Clones this instance.
        /// </summary>
        /// <returns></returns>
        public Queue<T> Clone()
        {
            var queue = new Queue<T>();
            foreach (var item in _queue)
            {
                queue._queue.Enqueue(item);
            }
            return queue;
        }

        object ICloneable.Clone()
        {
            return Clone();
        }
    }
}
