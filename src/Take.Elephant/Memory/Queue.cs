using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Memory
{
    /// <summary>
    /// Implements the <see cref="IQueue{T}"/> interface using the <see cref="System.Collections.Concurrent.ConcurrentQueue{T}"/> class.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class Queue<T> : IBlockingQueue<T>, IBatchSenderQueue<T>, IBatchReceiverQueue<T>, ICloneable
    {
        private readonly ConcurrentQueue<T> _queue;
        private readonly ConcurrentQueue<Tuple<TaskCompletionSource<T>, CancellationTokenRegistration>> _promisesQueue;
        private readonly object _syncRoot = new object();

        public Queue()
        {
            _queue = new ConcurrentQueue<T>();
            _promisesQueue = new ConcurrentQueue<Tuple<TaskCompletionSource<T>, CancellationTokenRegistration>>();
        }

        public virtual Task EnqueueAsync(T item, CancellationToken cancellationToken = default)
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

        public virtual async Task EnqueueBatchAsync(IEnumerable<T> items, CancellationToken cancellationToken = default)
        {
            if (items == null) throw new ArgumentNullException(nameof(items));

            foreach (var item in items)
            {
                await EnqueueAsync(item, cancellationToken).ConfigureAwait(false);
            }
        }

        public virtual Task<T> DequeueOrDefaultAsync(CancellationToken cancellationToken = default)
        {
            lock (_syncRoot)
            {
                T result;
                return _queue.TryDequeue(out result)
                    ? result.AsCompletedTask()
                    : default(T).AsCompletedTask();
            }
        }

        public virtual Task<long> GetLengthAsync(CancellationToken cancellationToken = default)
        {
            return ((long)_queue.Count).AsCompletedTask();
        }

        public virtual Task<T> DequeueAsync(CancellationToken cancellationToken)
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

        public virtual async Task<IEnumerable<T>> DequeueBatchAsync(int maxBatchSize, CancellationToken cancellationToken)
        {
            var items = new System.Collections.Generic.List<T>();
            while (items.Count < maxBatchSize)
            {
                var item = await DequeueOrDefaultAsync(cancellationToken);
                if (EqualityComparer<T>.Default.Equals(item, default)) break;
                items.Add(item);
            }

            return items;
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
