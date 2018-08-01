using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Redis
{
    /// <summary>
    /// Implements the <see cref="IListMap{TKey, TValue}"/> interface using Redis set data structure.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>   
    /// <typeparam name="TValue"></typeparam>   
    public class RedisListMap<TKey, TValue> : MapBase<TKey, IList<TValue>>, IListMap<TKey, TValue>
    {

        private readonly ISerializer<TValue> _serializer;
        private readonly bool _useScanOnEnumeration;

        #region constructors

        public RedisListMap(string mapName, string configuration, ISerializer<TValue> serializer, int db = 0, CommandFlags readFlags = CommandFlags.None, CommandFlags writeFlags = CommandFlags.None, bool useScanOnEnumeration = true)
            : this(mapName, StackExchange.Redis.ConnectionMultiplexer.Connect(configuration), serializer, db, readFlags, writeFlags)
        {
            _useScanOnEnumeration = useScanOnEnumeration;
        }

        public RedisListMap(string mapName, IConnectionMultiplexer connectionMultiplexer, ISerializer<TValue> serializer, int db = 0, CommandFlags readFlags = CommandFlags.None, CommandFlags writeFlags = CommandFlags.None, bool useScanOnEnumeration = true)
            : base(mapName, connectionMultiplexer, db, readFlags, writeFlags)
        {
            if (serializer == null) throw new ArgumentNullException(nameof(serializer));
            _serializer = serializer;
            _useScanOnEnumeration = useScanOnEnumeration;
        }

        public override Task<bool> ContainsKeyAsync(TKey key)
        {
            throw new NotImplementedException();
        }

        public override Task<IList<TValue>> GetValueOrDefaultAsync(TKey key)
        {
            throw new NotImplementedException();
        }

        #endregion

        public async Task<IList<TValue>> GetValueOrEmptyAsync(TKey key)
        {
            var database = GetDatabase();
            if (await database.KeyExistsAsync(GetRedisKey(key), ReadFlags).ConfigureAwait(false))
            {
                return CreateList(key);
            }

            return null;
        }

        public async override Task<bool> TryAddAsync(TKey key, IList<TValue> value, bool overwrite = false)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (value == null) throw new ArgumentNullException(nameof(value));

            var internalList = value as InternalList;
            if (internalList != null) return internalList.Key.Equals(key) && overwrite;

            var database = GetDatabase() as IDatabase;
            if (database == null) throw new NotSupportedException("The database instance type is not supported");

            var redisKey = GetRedisKey(key);
            if (await database.KeyExistsAsync(redisKey, ReadFlags) && !overwrite) return false;

            var transaction = database.CreateTransaction();
            var commandTasks = new List<Task>();
            if (overwrite) commandTasks.Add(transaction.KeyDeleteAsync(redisKey, WriteFlags));

            internalList = CreateList(key, transaction);

            var enumerableAsync = await value.AsEnumerableAsync().ConfigureAwait(false);
            await enumerableAsync.ForEachAsync(item =>
            {
                commandTasks.Add(internalList.AddAsync(item));
            }, CancellationToken.None).ConfigureAwait(false);

            var success = await transaction.ExecuteAsync(WriteFlags).ConfigureAwait(false);
            await Task.WhenAll(commandTasks).ConfigureAwait(false);
            return success;
        }

        protected virtual InternalList CreateList(TKey key, ITransaction transaction = null)
        {
            return new InternalList(key, GetRedisKey(key), _serializer, ConnectionMultiplexer, Db, ReadFlags, WriteFlags, transaction, _useScanOnEnumeration);
        }

        public override Task<bool> TryRemoveAsync(TKey key)
        {
            var database = GetDatabase();
            return database.KeyDeleteAsync(GetRedisKey(key), WriteFlags);
        }

        protected class InternalList : RedisList<TValue>
        {
            private readonly ITransaction _transaction;

            public InternalList(TKey key, string listName, ISerializer<TValue> serializer, IConnectionMultiplexer connectionMultiplexer, int db, CommandFlags readFlags, CommandFlags writeFlags, ITransaction transaction = null, bool useScanOnEnumeration = true)
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
