using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Take.Elephant.Redis
{
    public class RedisSortedSetMap<TKey, TItem> : MapBase<TKey, ISortedSet<TItem>>, ISortedSetMap<TKey, TItem>
    {
        private readonly ISerializer<TItem> _serializer;

        public RedisSortedSetMap(string mapName, string configuration, ISerializer<TItem> serializer, int db = 0, CommandFlags readFlags = CommandFlags.None, CommandFlags writeFlags = CommandFlags.None)
            : this(mapName, StackExchange.Redis.ConnectionMultiplexer.Connect(configuration), serializer, db, readFlags, writeFlags)
        {
        }

        public RedisSortedSetMap(string mapName, IConnectionMultiplexer connectionMultiplexer, ISerializer<TItem> serializer, int db = 0, CommandFlags readFlags = CommandFlags.None, CommandFlags writeFlags = CommandFlags.None)
            : base(mapName, connectionMultiplexer, db, readFlags, writeFlags)
        {
            _serializer = serializer;
        }

        public override Task<bool> ContainsKeyAsync(TKey key, CancellationToken cancelationToken = default)
        {
            var database = GetDatabase();
            return database.KeyExistsAsync(GetRedisKey(key), ReadFlags);
        }

        public override async Task<ISortedSet<TItem>> GetValueOrDefaultAsync(TKey key, CancellationToken cancelationToken = default)
        {
            var database = GetDatabase();
            if (await database.KeyExistsAsync(GetRedisKey(key), ReadFlags).ConfigureAwait(false))
            {
                return CreateList(key);
            }

            return null;
        }

        public Task<ISortedSet<TItem>> GetValueOrEmptyAsync(TKey key, CancellationToken cancelationToken = default)
        {
            return CreateList(key).AsCompletedTask<ISortedSet<TItem>>();
        }

        public override async Task<bool> TryAddAsync(TKey key, ISortedSet<TItem> value, bool overwrite = false, CancellationToken cancelationToken = default)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (value == null) throw new ArgumentNullException(nameof(value));

            var internalSet = value as InternalSortedSetList;
            if (internalSet != null) return internalSet.Key.Equals(key) && overwrite;

            var database = GetDatabase() as IDatabase;
            if (database == null) throw new NotSupportedException("The database instance type is not supported");

            var redisKey = GetRedisKey(key);
            if (await database.KeyExistsAsync(redisKey, ReadFlags) && !overwrite) return false;

            var transaction = database.CreateTransaction();
            var commandTasks = new List<Task>();
            if (overwrite) commandTasks.Add(transaction.KeyDeleteAsync(redisKey, WriteFlags));

            internalSet = CreateList(key, transaction);

            var enumerable = await value.AsEnumerableWithScoreAsync().ConfigureAwait(false);
            foreach (var item in enumerable)
            {
                commandTasks.Add(internalSet.AddAsync(item.Value, item.Key));
            }

            var success = await transaction.ExecuteAsync(WriteFlags).ConfigureAwait(false);
            await Task.WhenAll(commandTasks).ConfigureAwait(false);
            return success;
        }

        public override Task<bool> TryRemoveAsync(TKey key, CancellationToken cancelationToken = default)
        {
            var database = GetDatabase();
            return database.KeyDeleteAsync(GetRedisKey(key), WriteFlags);
        }

        private InternalSortedSetList CreateList(TKey key, ITransaction transaction = null)
        {
            return new InternalSortedSetList(key, GetRedisKey(key), _serializer, ConnectionMultiplexer, Db, ReadFlags, WriteFlags, transaction);
        }

        private class InternalSortedSetList : RedisSortedSet<TItem>
        {
            private readonly ITransaction _transaction;

            public InternalSortedSetList(TKey key, string listName, ISerializer<TItem> serializer, IConnectionMultiplexer connectionMultiplexer, int db, CommandFlags readFlags, CommandFlags writeFlags, ITransaction transaction = null)
                : base(listName, serializer, connectionMultiplexer, db, readFlags, writeFlags)
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