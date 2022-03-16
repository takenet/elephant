using StackExchange.Redis;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Redis
{
    public class RedisSetMap<TKey, TItem> : MapBase<TKey, ISet<TItem>>, ISetMap<TKey, TItem>
    {
        private const string EMPTY_SET_INDICATOR = "__ELEPHANT_EMPTY_SET_INDICATOR__";
        private readonly ISerializer<TItem> _serializer;
        private readonly bool _useScanOnEnumeration;
        private readonly bool _supportEmptySets;
        private readonly TimeSpan _emptyIndicatorExpiration;

        public RedisSetMap(string mapName,
                           string configuration,
                           ISerializer<TItem> serializer,
                           int db = 0,
                           CommandFlags readFlags = CommandFlags.None,
                           CommandFlags writeFlags = CommandFlags.None,
                           bool useScanOnEnumeration = true,
                           bool supportEmptySets = false,
                           TimeSpan? emptyIndicatorExpiration = default)
            : this(mapName, StackExchange.Redis.ConnectionMultiplexer.Connect(configuration), serializer, db, readFlags, writeFlags, useScanOnEnumeration, supportEmptySets, emptyIndicatorExpiration)
        {
        }

        public RedisSetMap(string mapName,
                           IConnectionMultiplexer connectionMultiplexer,
                           ISerializer<TItem> serializer,
                           int db = 0,
                           CommandFlags readFlags = CommandFlags.None,
                           CommandFlags writeFlags = CommandFlags.None,
                           bool useScanOnEnumeration = true,
                           bool supportEmptySets = false,
                           TimeSpan? emptyIndicatorExpiration = default)
            : base(mapName, connectionMultiplexer, db, readFlags, writeFlags)
        {
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            _useScanOnEnumeration = useScanOnEnumeration;
            _supportEmptySets = supportEmptySets;
            _emptyIndicatorExpiration = emptyIndicatorExpiration ?? TimeSpan.FromMinutes(15);
        }

        public bool SupportsEmptySets => _supportEmptySets;

        public override async Task<bool> TryAddAsync(TKey key,
            ISet<TItem> value,
            bool overwrite = false,
            CancellationToken cancellationToken = default)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (value is InternalSet internalSet)
            {
                return internalSet.Key.Equals(key) && overwrite;
            }

            if (!(GetDatabase() is IDatabase database))
            {
                throw new NotSupportedException("The database instance type is not supported");
            }

            var redisKey = GetRedisKey(key);
            if (await database.KeyExistsAsync(redisKey, ReadFlags).ConfigureAwait(false) && !overwrite)
            {
                return false;
            }

            var transaction = database.CreateTransaction();

            if (value == null && _supportEmptySets)
            {
                _ = transaction.StringSetAsync(GetEmptySetIndicatorForKey(redisKey), true.ToString(), _emptyIndicatorExpiration).ConfigureAwait(false);
                return await transaction.ExecuteAsync(WriteFlags).ConfigureAwait(false);
            }
            else if (value == null)
            {
                return false;
            }

            var commandTasks = new List<Task>();
            if (overwrite)
            {
                commandTasks.Add(transaction.KeyDeleteAsync(redisKey, WriteFlags));
            }

            internalSet = CreateSet(key, transaction);

            var enumerableAsync = value.AsEnumerableAsync(cancellationToken);
            int itemsAdded = 0;
            await enumerableAsync.ForEachAsync(item =>
            {
                itemsAdded++;
                commandTasks.Add(internalSet.AddAsync(item, cancellationToken));
            }, cancellationToken).ConfigureAwait(false);

            if (_supportEmptySets)
            {
                commandTasks.Add(transaction.StringSetAsync(GetEmptySetIndicatorForKey(redisKey), (itemsAdded > 0).ToString(), _emptyIndicatorExpiration));
            }

            var success = await transaction.ExecuteAsync(WriteFlags).ConfigureAwait(false);
            await Task.WhenAll(commandTasks).ConfigureAwait(false);

            return success;
        }

        public override async Task<ISet<TItem>> GetValueOrDefaultAsync(TKey key,
            CancellationToken cancellationToken = default)
        {
            var database = GetDatabase();
            var redisKey = GetRedisKey(key);

            if ((_supportEmptySets
                && bool.TryParse((await database.StringGetAsync(GetEmptySetIndicatorForKey(redisKey))).ToString(), out bool isEmpty)
                && isEmpty)
                || await database.KeyExistsAsync(redisKey, ReadFlags).ConfigureAwait(false))
            {
                // TODO can't return memory here, as adds won't reflect in the cache
                return CreateSet(key);
                //return new Memory.Set<TItem>();
            }

            return null;
        }

        public virtual Task<ISet<TItem>> GetValueOrEmptyAsync(TKey key, CancellationToken cancellationToken = default)
        {
            return CreateSet(key).AsCompletedTask<ISet<TItem>>();
        }

        public override async Task<bool> TryRemoveAsync(TKey key, CancellationToken cancellationToken = default)
        {
            var database = GetDatabase();
            var redisKey = GetRedisKey(key);
            var removed = await database.KeyDeleteAsync(redisKey, WriteFlags);

            if (_supportEmptySets && removed)
            {
                await database.KeyDeleteAsync(GetEmptySetIndicatorForKey(redisKey), WriteFlags);
            }

            return removed;
        }

        public override async Task<bool> ContainsKeyAsync(TKey key, CancellationToken cancellationToken = default)
        {
            var database = GetDatabase();
            return await database.KeyExistsAsync(GetRedisKey(key), ReadFlags);
        }

        protected virtual InternalSet CreateSet(TKey key, ITransaction transaction = null)
        {
            return new InternalSet(key, GetRedisKey(key), _serializer, ConnectionMultiplexer, Db, ReadFlags, WriteFlags, transaction, _useScanOnEnumeration);
        }

        private RedisKey GetEmptySetIndicatorForKey(string key) => $"{key}{EMPTY_SET_INDICATOR}";

        protected class InternalSet : RedisSet<TItem>
        {
            private readonly ITransaction _transaction;

            public InternalSet(TKey key, string setName, ISerializer<TItem> serializer, IConnectionMultiplexer connectionMultiplexer, int db, CommandFlags readFlags, CommandFlags writeFlags, ITransaction transaction = null, bool useScanOnEnumeration = true)
                : base(setName, connectionMultiplexer, serializer, db, readFlags, writeFlags, useScanOnEnumeration)
            {
                if (key == null)
                {
                    throw new ArgumentNullException(nameof(key));
                }

                Key = key;

                _transaction = transaction;
            }

            public TKey Key { get; }

            protected override IDatabaseAsync GetDatabase()
            {
                return _transaction ?? base.GetDatabase();
            }
        }
    }
}