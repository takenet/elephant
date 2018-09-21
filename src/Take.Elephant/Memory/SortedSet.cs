using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Memory
{
    /// <summary>
    /// Implements the <see cref="ISortedSet{T}"/> interface using the <see cref="System.Collections.Generic.SortedList{double, T}"/> class.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class SortedSet<T> : ISortedSet<T>
    {
        private readonly SortedList<double, T> _sortedList;
        private readonly ConcurrentQueue<Tuple<TaskCompletionSource<T>, CancellationTokenRegistration>> _promisesQueue;
        private readonly object _syncRoot = new object();

        public SortedSet() : this(new SortedList<double, T>(new DuplicateKeyComparer<double>()))
        { }

        private SortedSet(SortedList<double, T> sortedList)
        {
            _sortedList = sortedList;
            _promisesQueue = new ConcurrentQueue<Tuple<TaskCompletionSource<T>, CancellationTokenRegistration>>();
        }

        public Task<IAsyncEnumerable<T>> AsEnumerableAsync()
            => Task.FromResult<IAsyncEnumerable<T>>(new AsyncEnumerableWrapper<T>(_sortedList.Values));

        public Task<T> DequeueMaxAsync(CancellationToken cancellationToken)
        {
            T item;
            lock (_syncRoot)
            {
                cancellationToken.ThrowIfCancellationRequested();
                item = _sortedList.Values.LastOrDefault();
                if (!EqualityComparer<T>.Default.Equals(item, default(T)))
                {
                    _sortedList.RemoveAt(_sortedList.Count() - 1);
                }
                else
                {
                    var promise = new TaskCompletionSource<T>();
                    var registration = cancellationToken.Register(() => promise.TrySetCanceled());
                    _promisesQueue.Enqueue(Tuple.Create(promise, registration));
                    return promise.Task;
                }
            }
            return item.AsCompletedTask();
        }

        public Task<T> DequeueMaxOrDefaultAsync()
        {
            T item;
            lock (_syncRoot)
            {
                item = _sortedList.Values.LastOrDefault();
                if (!EqualityComparer<T>.Default.Equals(item, default(T)))
                {
                    _sortedList.RemoveAt(_sortedList.Count() - 1);
                }
            }
            return item.AsCompletedTask();
        }

        public Task<T> DequeueMinAsync(CancellationToken cancellationToken)
        {
            T item;
            lock (_syncRoot)
            {
                cancellationToken.ThrowIfCancellationRequested();
                item = _sortedList.Values.FirstOrDefault();
                if (!EqualityComparer<T>.Default.Equals(item, default(T)))
                {
                    _sortedList.RemoveAt(0);
                }
                else
                {
                    var promise = new TaskCompletionSource<T>();
                    var registration = cancellationToken.Register(() => promise.TrySetCanceled());
                    _promisesQueue.Enqueue(Tuple.Create(promise, registration));
                    return promise.Task;
                }
            }
            return item.AsCompletedTask();
        }

        public Task<T> DequeueMinOrDefaultAsync()
        {
            T item;
            lock (_syncRoot)
            {
                item = _sortedList.Values.FirstOrDefault();
                if (!EqualityComparer<T>.Default.Equals(item, default(T)))
                {
                    _sortedList.RemoveAt(0);
                }
            }
            return item.AsCompletedTask();
        }

        public Task EnqueueAsync(T item, double score)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));
            lock (_syncRoot)
            {
                Tuple<TaskCompletionSource<T>, CancellationTokenRegistration> promise;
                do
                {
                    if (_promisesQueue.TryDequeue(out promise)
                        && !promise.Item1.Task.IsCanceled
                        && promise.Item1.TrySetResult(item))
                    {
                        promise.Item2.Dispose();
                        return TaskUtil.CompletedTask;
                    }
                }
                while (promise != null);

                _sortedList.Add(score, item);
                return TaskUtil.CompletedTask;
            }
        }

        public Task<long> GetLengthAsync()
        {
            return _sortedList.LongCount().AsCompletedTask();
        }

        public async Task<IAsyncEnumerable<T>> RangeByRankAsync(long initial = 0, long end = -1)
        {
            var length = await GetLengthAsync();
            if (initial < 0)
            {
                initial = length - initial;
            }
            if (end < 0)
            {
                end = length - end;
            }

            System.Collections.Generic.ICollection<T> list = new System.Collections.Generic.List<T>();
            for (var i = initial; i < end; i++)
            {
                if (i < length && i >= 0)
                {
                    list.Add(_sortedList.Values[(int)i]);
                }
            }
            return await Task.FromResult<IAsyncEnumerable<T>>(new AsyncEnumerableWrapper<T>(list));
        }

        public Task<bool> RemoveAsync(T value)
        {
            var item = _sortedList.IndexOfValue(value);
            _sortedList.RemoveAt(item);
            return true.AsCompletedTask();
        }
    }

    /// <summary>
    /// Workaround for the SortedList have duplicate keys
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    public class DuplicateKeyComparer<TKey> :
             IComparer<TKey> where TKey : IComparable
    {
        public int Compare(TKey x, TKey y)
        {
            int result = x.CompareTo(y);

            if (result == 0)
                return 1;
            else
                return result;
        }
    }
}