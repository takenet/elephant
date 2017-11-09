using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace Takenet.Elephant.Memory
{
    /// <summary>
    /// Implements the <see cref="ISet{T}"/> interface using the <see cref="System.Collections.Generic.HashSet{T}"/> class.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class Set<T> : Collection<T>, ISet<T>
    {
        private readonly HashSet<T> _hashSet;
        private object _syncRoot = new object();


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
            _hashSet = hashSet;
        }


        public virtual Task AddAsync(T value)
        {
            lock (_syncRoot)
            {
                if (value == null) throw new ArgumentNullException(nameof(value));
                if (_hashSet.Contains(value))
                {
                    _hashSet.Remove(value);
                }

                _hashSet.Add(value);
            }
            return TaskUtil.CompletedTask;
        }

        public virtual Task<bool> TryRemoveAsync(T value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            lock (_syncRoot)
            {
                return Task.FromResult(_hashSet.Remove(value));
            }
        }
       
        public virtual Task<bool> ContainsAsync(T value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            return Task.FromResult(_hashSet.Contains(value));
        }

        
    }
}
