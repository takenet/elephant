using StackExchange.Redis;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Redis
{
    /// <summary>
    /// Implements the <see cref="IListMap{TKey, TItem}"/> interface using Redis set data structure.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TItem"></typeparam>
    public class RedisListMap<TKey, TItem> : MapBase<TKey, IList<TItem>>, IListMap<TKey, TItem>
    {
        private readonly ISerializer<TItem> _serializer;

        public RedisListMap(string mapName, string configuration, ISerializer<TItem> serializer, int db = 0, CommandFlags readFlags = CommandFlags.None, CommandFlags writeFlags = CommandFlags.None)
            : this(mapName, StackExchange.Redis.ConnectionMultiplexer.Connect(configuration), serializer, db, readFlags, writeFlags)
        { }

        public RedisListMap(string mapName, IConnectionMultiplexer connectionMultiplexer, ISerializer<TItem> serializer, int db = 0, CommandFlags readFlags = CommandFlags.None, CommandFlags writeFlags = CommandFlags.None)
            : base(mapName, connectionMultiplexer, db, readFlags, writeFlags)
        {
            if (serializer == null) throw new ArgumentNullException(nameof(serializer));
            _serializer = serializer;
        }

        public override Task<bool> ContainsKeyAsync(TKey key, CancellationToken cancellationToken = default)
        {
            var database = GetDatabase();
            return database.KeyExistsAsync(GetRedisKey(key), ReadFlags);
        }

        public async override Task<IList<TItem>> GetValueOrDefaultAsync(TKey key,
            CancellationToken cancellationToken = default)
        {
            var database = GetDatabase();
            if (await database.KeyExistsAsync(GetRedisKey(key), ReadFlags).ConfigureAwait(false))
            {
                return CreateList(key);
            }

            return null;
        }

        public Task<IList<TItem>> GetValueOrEmptyAsync(TKey key)
        {
            return CreateList(key).AsCompletedTask<IList<TItem>>();
        }

        public async override Task<bool> TryAddAsync(TKey key,
            IList<TItem> value,
            bool overwrite = false,
            CancellationToken cancellationToken = default)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (value == null) throw new ArgumentNullException(nameof(value));

            var internalSet = value as InternalList;
            if (internalSet != null) return internalSet.Key.Equals(key) && overwrite;

            var database = GetDatabase() as IDatabase;
            if (database == null) throw new NotSupportedException("The database instance type is not supported");

            var redisKey = GetRedisKey(key);
            if (await database.KeyExistsAsync(redisKey, ReadFlags) && !overwrite) return false;

            var transaction = database.CreateTransaction();
            var commandTasks = new System.Collections.Generic.List<Task>();
            if (overwrite) commandTasks.Add(transaction.KeyDeleteAsync(redisKey, WriteFlags));

            internalSet = CreateList(key, transaction);

            var enumerableAsync = await value.AsEnumerableAsync().ConfigureAwait(false);
            await enumerableAsync.ForEachAsync(item =>
            {
                commandTasks.Add(internalSet.AddAsync(item));
            }, CancellationToken.None).ConfigureAwait(false);

            var success = await transaction.ExecuteAsync(WriteFlags).ConfigureAwait(false);
            await Task.WhenAll(commandTasks).ConfigureAwait(false);
            return success;
        }

        public override Task<bool> TryRemoveAsync(TKey key, CancellationToken cancellationToken = default)
        {
            var database = GetDatabase();
            return database.KeyDeleteAsync(GetRedisKey(key), WriteFlags);
        }

        protected virtual InternalList CreateList(TKey key, ITransaction transaction = null)
        {
            return new InternalList(key, GetRedisKey(key), _serializer, ConnectionMultiplexer, Db, ReadFlags, WriteFlags, transaction);
        }

        protected class InternalList : RedisList<TItem>
        {
            private readonly ITransaction _transaction;

            public InternalList(TKey key, string listName, ISerializer<TItem> serializer, IConnectionMultiplexer connectionMultiplexer, int db, CommandFlags readFlags, CommandFlags writeFlags, ITransaction transaction = null)
                : base(listName, connectionMultiplexer, serializer, db, readFlags, writeFlags)
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