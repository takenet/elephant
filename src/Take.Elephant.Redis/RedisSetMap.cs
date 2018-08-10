using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Redis
{
    public class RedisSetMap<TKey, TItem> : MapBase<TKey, ISet<TItem>>, ISetMap<TKey, TItem>
    {
        private readonly ISerializer<TItem> _serializer;
        private readonly bool _useScanOnEnumeration;

        public RedisSetMap(string mapName, string configuration, ISerializer<TItem> serializer, int db = 0, CommandFlags readFlags = CommandFlags.None, CommandFlags writeFlags = CommandFlags.None, bool useScanOnEnumeration = true)
            : this(mapName, StackExchange.Redis.ConnectionMultiplexer.Connect(configuration), serializer, db, readFlags, writeFlags)
        {
            _useScanOnEnumeration = useScanOnEnumeration;
        }

        public RedisSetMap(string mapName, IConnectionMultiplexer connectionMultiplexer, ISerializer<TItem> serializer, int db = 0, CommandFlags readFlags = CommandFlags.None, CommandFlags writeFlags = CommandFlags.None, bool useScanOnEnumeration = true)
            : base(mapName, connectionMultiplexer, db, readFlags, writeFlags)
        {
            if (serializer == null) throw new ArgumentNullException(nameof(serializer));
            _serializer = serializer;
            _useScanOnEnumeration = useScanOnEnumeration;
        }

        public override async Task<bool> TryAddAsync(TKey key,
            ISet<TItem> value,
            bool overwrite = false,
            CancellationToken cancellationToken = default)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (value == null) throw new ArgumentNullException(nameof(value));

            var internalSet = value as InternalSet;
            if (internalSet != null) return internalSet.Key.Equals(key) && overwrite;

            var database = GetDatabase() as IDatabase;
            if (database == null) throw new NotSupportedException("The database instance type is not supported");

            var redisKey = GetRedisKey(key);
            if (await database.KeyExistsAsync(redisKey, ReadFlags) && !overwrite) return false;

            var transaction = database.CreateTransaction();
            var commandTasks = new List<Task>();
            if (overwrite) commandTasks.Add(transaction.KeyDeleteAsync(redisKey, WriteFlags));

            internalSet = CreateSet(key, transaction);

            var enumerableAsync = await value.AsEnumerableAsync().ConfigureAwait(false);
            await enumerableAsync.ForEachAsync(item =>
            {
                commandTasks.Add(internalSet.AddAsync(item));
            }, CancellationToken.None).ConfigureAwait(false);

            var success = await transaction.ExecuteAsync(WriteFlags).ConfigureAwait(false);
            await Task.WhenAll(commandTasks).ConfigureAwait(false);
            return success;
        }

        public override async Task<ISet<TItem>> GetValueOrDefaultAsync(TKey key,
            CancellationToken cancellationToken = default)
        {
            var database = GetDatabase();
            if (await database.KeyExistsAsync(GetRedisKey(key), ReadFlags).ConfigureAwait(false))
            {
                return CreateSet(key);
            }

            return null;
        }

        public virtual Task<ISet<TItem>> GetValueOrEmptyAsync(TKey key, CancellationToken cancellationToken = default)
        {
            return CreateSet(key).AsCompletedTask<ISet<TItem>>();
        }

        public override Task<bool> TryRemoveAsync(TKey key, CancellationToken cancellationToken = default)
        {
            var database = GetDatabase();
            return database.KeyDeleteAsync(GetRedisKey(key), WriteFlags);
        }

        public override Task<bool> ContainsKeyAsync(TKey key, CancellationToken cancellationToken = default)
        {
            var database = GetDatabase();
            return database.KeyExistsAsync(GetRedisKey(key), ReadFlags);
        }

        protected virtual InternalSet CreateSet(TKey key, ITransaction transaction = null)
        {
            return new InternalSet(key, GetRedisKey(key), _serializer, ConnectionMultiplexer, Db, ReadFlags, WriteFlags, transaction, _useScanOnEnumeration);
        }

        protected class InternalSet : RedisSet<TItem>
        {
            private readonly ITransaction _transaction;

            public InternalSet(TKey key, string setName, ISerializer<TItem> serializer, IConnectionMultiplexer connectionMultiplexer, int db, CommandFlags readFlags, CommandFlags writeFlags, ITransaction transaction = null, bool useScanOnEnumeration = true)
                : base(setName, connectionMultiplexer, serializer, db, readFlags, writeFlags, useScanOnEnumeration)
            {
                if (key == null) throw new ArgumentNullException(nameof(key));
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