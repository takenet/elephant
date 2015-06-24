using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Takenet.Elephant.Redis
{
    public class RedisSetMap<TKey, TItem> : MapBase<TKey, ISet<TItem>>, ISetMap<TKey, TItem>
    {
        private readonly ISerializer<TItem> _serializer;

        public RedisSetMap(string mapName, string configuration, ISerializer<TItem> serializer, int db = 0)
            : base(mapName, configuration, db)
        {
            if (serializer == null) throw new ArgumentNullException(nameof(serializer));
            _serializer = serializer;
        }

        internal RedisSetMap(string mapName, ConnectionMultiplexer connectionMultiplexer, ISerializer<TItem> serializer, int db)
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
            if (await database.KeyExistsAsync(redisKey) && !overwrite) return false;

            var transaction = database.CreateTransaction();
            var commandTasks = new List<Task>();
            if (overwrite) commandTasks.Add(transaction.KeyDeleteAsync(redisKey));

            internalSet = CreateSet(key, transaction);

            foreach (var item in await value.AsEnumerableAsync().ConfigureAwait(false))
            {
                commandTasks.Add(internalSet.AddAsync(item));
            }            
            var success = await transaction.ExecuteAsync().ConfigureAwait(false);
            await Task.WhenAll(commandTasks).ConfigureAwait(false);
            return success;
        }

        public override async Task<ISet<TItem>> GetValueOrDefaultAsync(TKey key)
        {
            var database = GetDatabase();
            if (await database.KeyExistsAsync(GetRedisKey(key)).ConfigureAwait(false))
            {
                return CreateSet(key);
            }

            return null;
        }

        public override Task<bool> TryRemoveAsync(TKey key)
        {
            var database = GetDatabase();
            return database.KeyDeleteAsync(GetRedisKey(key));            
        }

        public override Task<bool> ContainsKeyAsync(TKey key)
        {
            var database = GetDatabase();
            return database.KeyExistsAsync(GetRedisKey(key));
        }

        protected InternalSet CreateSet(TKey key, ITransaction transaction = null, bool useScanOnEnumeration = true)
        {
            return new InternalSet(key, GetRedisKey(key), _serializer, _connectionMultiplexer, _db, transaction, useScanOnEnumeration);
        }

        protected class InternalSet : RedisSet<TItem>
        {
            private readonly ITransaction _transaction;

            public InternalSet(TKey key, string setName, ISerializer<TItem> serializer, ConnectionMultiplexer connectionMultiplexer, int db, ITransaction transaction = null, bool useScanOnEnumeration = true)
                : base(setName, connectionMultiplexer, serializer, db, useScanOnEnumeration)
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
