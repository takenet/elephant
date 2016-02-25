using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Takenet.Elephant.Redis
{
    public class RedisSetMap<TKey, TItem> : MapBase<TKey, ISet<TItem>>, ISetMap<TKey, TItem>
    {
        private readonly ISerializer<TItem> _serializer;

        public RedisSetMap(string mapName, string configuration, ISerializer<TItem> serializer, int db = 0)
            : this(mapName, ConnectionMultiplexer.Connect(configuration), serializer, db)
        {
            
        }

        public RedisSetMap(string mapName, IConnectionMultiplexer connectionMultiplexer, ISerializer<TItem> serializer, int db)
            : base(mapName, connectionMultiplexer, db)
        {
            if (serializer == null) throw new ArgumentNullException(nameof(serializer));
            _serializer = serializer;
        }

        public override async Task<bool> TryAddAsync(TKey key, ISet<TItem> value, bool overwrite = false)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (value == null) throw new ArgumentNullException(nameof(value));

            var internalSet = value as InternalSet;
            if (internalSet != null) return internalSet.Key.Equals(key) && overwrite;            

            var database = GetDatabase() as IDatabase;
            if (database == null) throw new NotSupportedException("The database instance type is not supported");

            var redisKey = GetRedisKey(key);
            if (await database.KeyExistsAsync(redisKey, GetFlags()) && !overwrite) return false;

            var transaction = database.CreateTransaction();
            var commandTasks = new List<Task>();
            if (overwrite) commandTasks.Add(transaction.KeyDeleteAsync(redisKey, GetFlags()));

            internalSet = CreateSet(key, transaction);

            var enumerableAsync = await value.AsEnumerableAsync().ConfigureAwait(false);
            await enumerableAsync.ForEachAsync(item =>
            {
                commandTasks.Add(internalSet.AddAsync(item));
            }, CancellationToken.None).ConfigureAwait(false);

            var success = await transaction.ExecuteAsync(GetFlags()).ConfigureAwait(false);
            await Task.WhenAll(commandTasks).ConfigureAwait(false);
            return success;
        }

        public override async Task<ISet<TItem>> GetValueOrDefaultAsync(TKey key)
        {
            var database = GetDatabase();
            if (await database.KeyExistsAsync(GetRedisKey(key), GetFlags()).ConfigureAwait(false))
            {
                return CreateSet(key);
            }

            return null;
        }

        public override Task<bool> TryRemoveAsync(TKey key)
        {
            var database = GetDatabase();
            return database.KeyDeleteAsync(GetRedisKey(key), GetFlags());            
        }

        public override Task<bool> ContainsKeyAsync(TKey key)
        {
            var database = GetDatabase();
            return database.KeyExistsAsync(GetRedisKey(key), GetFlags());
        }

        protected InternalSet CreateSet(TKey key, ITransaction transaction = null, bool useScanOnEnumeration = true)
        {
            return new InternalSet(key, GetRedisKey(key), _serializer, _connectionMultiplexer, _db, GetFlags(), transaction, useScanOnEnumeration);
        }

        protected class InternalSet : RedisSet<TItem>
        {
            private readonly CommandFlags _flags;
            private readonly ITransaction _transaction;

            public InternalSet(TKey key, string setName, ISerializer<TItem> serializer, IConnectionMultiplexer connectionMultiplexer, int db, CommandFlags flags, ITransaction transaction = null, bool useScanOnEnumeration = true)
                : base(setName, connectionMultiplexer, serializer, db, useScanOnEnumeration)
            {                
                if (key == null) throw new ArgumentNullException(nameof(key));
                Key = key;
                _flags = flags;
                _transaction = transaction;
            }

            public TKey Key { get; }

            protected override IDatabaseAsync GetDatabase()
            {
                return _transaction ?? base.GetDatabase();
            }

            protected override CommandFlags GetFlags() => _flags;
        }
    }
}
