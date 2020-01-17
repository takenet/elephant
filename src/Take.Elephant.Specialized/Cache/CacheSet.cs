using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Specialized.Synchronization;

namespace Take.Elephant.Specialized.Cache
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

        public virtual Task AddAsync(T value, CancellationToken cancellationToken = default) => ExecuteWriteFunc(s => s.AddAsync(value, cancellationToken));

        public virtual Task<bool> TryRemoveAsync(T value, CancellationToken cancellationToken = default) => ExecuteWriteFunc(s => s.TryRemoveAsync(value, cancellationToken));

        public virtual Task<IAsyncEnumerable<T>> AsEnumerableAsync(CancellationToken cancellationToken = default) => ExecuteQueryFunc(s => s.AsEnumerableAsync(cancellationToken));

        public virtual Task<bool> ContainsAsync(T value, CancellationToken cancellationToken = default) => ExecuteQueryFunc(s => s.ContainsAsync(value, cancellationToken));

        public virtual Task<long> GetLengthAsync(CancellationToken cancellationToken = default) => ExecuteQueryFunc(s => s.GetLengthAsync(cancellationToken));
    }
}
