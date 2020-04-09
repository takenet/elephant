using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Memory
{
    /// <summary>
    /// Implements the <see cref="ISet{T}"/> interface using the <see cref="System.Collections.Generic.HashSet{T}"/> class.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class Set<T> : Collection<T>, ISet<T>
    {
        
        private readonly object _syncRoot = new object();

        public Set()
            : this(EqualityComparer<T>.Default)
        {
        }

        public Set(IEqualityComparer<T> equalityComparer)
            : this(new HashSet<T>(equalityComparer))
        {
        }

        public Set(IEnumerable<T> collection)
            : this(collection, EqualityComparer<T>.Default)
        {
        }

        public Set(IEnumerable<T> collection, IEqualityComparer<T> equalityComparer)
            : this(new HashSet<T>(collection, equalityComparer))
        {
        }

        private Set(HashSet<T> hashSet)
            : base(hashSet)
        {
            HashSet = hashSet;
        }
        
        protected internal HashSet<T> HashSet { get; }

        public virtual Task AddAsync(T value, CancellationToken cancellationToken = default)
        {
            lock (_syncRoot)
            {
                if (value == null) throw new ArgumentNullException(nameof(value));
                if (HashSet.Contains(value))
                {
                    HashSet.Remove(value);
                }

                HashSet.Add(value);
            }
            return TaskUtil.CompletedTask;
        }

        public virtual Task<bool> TryRemoveAsync(T value, CancellationToken cancellationToken = default)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            lock (_syncRoot)
            {
                return Task.FromResult(HashSet.Remove(value));
            }
        }

        public virtual Task<bool> ContainsAsync(T value, CancellationToken cancellationToken = default)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            return Task.FromResult(HashSet.Contains(value));
        }
    }
}