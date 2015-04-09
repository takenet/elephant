using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence.Memory
{
    /// <summary>
    /// Implements the <see cref="IQueue{T}"/> interface using the <see cref="System.Collections.Concurrent.ConcurrentQueue{T}"/> class.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class Queue<T> : IQueue<T>, ICloneable
    {
        private readonly ConcurrentQueue<T> _queue;

        public Queue()
        {            
            _queue = new ConcurrentQueue<T>();
        }

        #region IQueue<T> Members

        public Task EnqueueAsync(T item)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));
            _queue.Enqueue(item);
            return TaskUtil.CompletedTask;
        }

        public Task<T> DequeueOrDefaultAsync()
        {            
            T result;
            return _queue.TryDequeue(out result) ? 
                result.AsCompletedTask() : 
                default(T).AsCompletedTask();
        }

        public Task<long> GetLengthAsync()
        {
            return ((long)_queue.Count).AsCompletedTask();
        }

        #endregion

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
