using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace Take.Elephant.EntityFramework
{
    public class EntityFrameworkMap<TKey, TValue> : IMap<TKey, TValue> where TValue : class
    {
        private readonly Func<TValue, TKey> _selectKeyFunc;

        public EntityFrameworkMap(DbContext dbContext, Func<TValue, TKey> selectKeyFunc)
        {
            _selectKeyFunc = selectKeyFunc ?? throw new ArgumentNullException(nameof(selectKeyFunc));
            DbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
            DbSet = dbContext.Set<TValue>();
        }
        
        public DbSet<TValue> DbSet { get; }
        
        public DbContext DbContext { get; }

        public Task<bool> TryAddAsync(TKey key, TValue value, bool overwrite = false, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<TValue> GetValueOrDefaultAsync(TKey key, CancellationToken cancellationToken = default)
        {
            return DbSet.FindAsync(key, cancellationToken).AsTask();
        }

        public async Task<bool> TryRemoveAsync(TKey key, CancellationToken cancellationToken = default)
        {
            var value = await GetValueOrDefaultAsync(key, cancellationToken).ConfigureAwait(false);
            if (value == null) return false;
            DbSet.Remove(value);
            return await DbContext.SaveChangesAsync(cancellationToken).ConfigureAwait(false) != 0;
        }

        public Task<bool> ContainsKeyAsync(TKey key, CancellationToken cancellationToken = default)
        {
            return DbSet.AsQueryable().AnyAsync(i => _selectKeyFunc(i).Equals(key), cancellationToken);
        }
    }
}