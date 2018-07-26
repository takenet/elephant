using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Memory
{
    /// <summary>
    /// Implements the <see cref="ISortedSet{T}"/> interface using the <see cref="System.Collections.Generic.HashSet{T}"/> class.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class SortedSet<T> : Collection<T>, ISortedSet<T>
    {
        private readonly SortedList<float, T> _sortedList;

        protected SortedSet(System.Collections.Generic.ICollection<T> collection) : base(collection)
        {
        }

        public SortedSet()
        {
            _sortedList = new SortedList<float, T>(new DuplicateKeyComparer<float>());
        }

        public Task<IAsyncEnumerable<T>> AsEnumerableAsync()
        {
            throw new NotImplementedException();
        }

        public Task<T> DequeueMaxAsync(CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public Task<T> DequeueMaxOrDefaultAsync()
        {
            throw new NotImplementedException();
        }

        public Task<T> DequeueMinAsync(CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public Task<T> DequeueMinOrDefaultAsync()
        {
            throw new NotImplementedException();
        }

        public Task EnqueueAsync(T item, float score)
        {
            throw new NotImplementedException();
        }

        public Task<long> GetLengthAsync()
        {
            throw new NotImplementedException();
        }
    }

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
