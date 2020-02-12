using System;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace Take.Elephant.EntityFramework
{
    public class EntityFrameworkMap<TKey, TValue> : IMap<TKey, TValue> where TValue : class
    {
        private readonly Expression<Func<TValue, TKey>> _keySelector;
        private readonly string _keyPropertyName;
        private readonly Func<TValue, TKey> _getKeyFunc;
        private readonly Action<TValue, TKey> _setKeyFunc;

        public EntityFrameworkMap(
            DbContext dbContext,
            DbSet<TValue> dbSet,
            Expression<Func<TValue, TKey>> keySelector)
        {
            DbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
            DbSet = dbSet ?? throw new ArgumentNullException(nameof(dbSet));
            _keySelector = keySelector ?? throw new ArgumentNullException(nameof(keySelector));
            if (!(_keySelector.Body is MemberExpression memberExpression))
            {
                throw new ArgumentException("Invalid key selector expression", nameof(keySelector));
            }

            if (memberExpression.Member.MemberType != MemberTypes.Property)
            {
                throw new ArgumentException("Key should be a property", nameof(keySelector));
            }
            
            _keyPropertyName = memberExpression.Member.Name;
            _getKeyFunc = TypeUtil.BuildGetAccessor(keySelector);
            _setKeyFunc = TypeUtil.BuildSetAccessor(keySelector);
        }
        
        protected DbSet<TValue> DbSet { get; }
        
        protected DbContext DbContext { get; }

        public async Task<bool> TryAddAsync(TKey key, TValue value, bool overwrite = false, CancellationToken cancellationToken = default)
        {
            _setKeyFunc(value, key);

            if (!overwrite &&
                await ContainsKeyAsync(key, cancellationToken).ConfigureAwait(false))
            {
                return false;
            }
            
            await DbSet.AddAsync(value, cancellationToken).ConfigureAwait(false);
            await DbContext.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
            return true;
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
            var equalsExpression =
                Expression.Lambda<Func<TValue, bool>>(
                    Expression.Equal(
                        Expression.Property(Expression.Parameter(typeof(TValue), "k"), _keyPropertyName),
                        Expression.Constant(key)));
            
            return DbSet.AsQueryable().AnyAsync(equalsExpression, cancellationToken);
        }
    }
}