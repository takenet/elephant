using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Take.Elephant.Specialized
{
    public class CacheSet<T> : CacheStrategy<ISet<T>>, ISet<T>
    {
        public CacheSet(ISet<T> source, ISet<T> cache, TimeSpan synchronizationTimeout, TimeSpan cacheExpiration = default(TimeSpan))
            : base(source, cache, new OverwriteSetSynchronizer<T>(synchronizationTimeout), cacheExpiration)
        {
        }

        protected CacheSet(ISet<T> source, ISet<T> cache, ISynchronizer<ISet<T>> synchronizer, TimeSpan cacheExpiration = default(TimeSpan)) 
            : base(source, cache, synchronizer, cacheExpiration)
        {
        }

        public virtual Task AddAsync(T value) => ExecuteWriteFunc(s => s.AddAsync(value));

        public virtual Task<bool> TryRemoveAsync(T value) => ExecuteWriteFunc(s => s.TryRemoveAsync(value));

        public virtual Task<IAsyncEnumerable<T>> AsEnumerableAsync() => ExecuteQueryFunc(s => s.AsEnumerableAsync());

        public virtual Task<bool> ContainsAsync(T value) => ExecuteQueryFunc(s => s.ContainsAsync(value));

        public virtual Task<long> GetLengthAsync() => ExecuteQueryFunc(s => s.GetLengthAsync());
    }
}
