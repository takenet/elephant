using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;
using StackExchange.Redis.KeyspaceIsolation;

namespace Takenet.SimplePersistence.Redis
{
    public class RedisSetMap<TKey, TItem> : MapBase<TKey, ISet<TItem>>, ISetMap<TKey, TItem>
    {
        private readonly ISerializer<TItem> _serializer;

        public RedisSetMap(string name, string configuration, ISerializer<TItem> serializer)
            : base(name, configuration)
        {
            if (serializer == null) throw new ArgumentNullException(nameof(serializer));
            _serializer = serializer;
        }

        public RedisSetMap(string name, ConnectionMultiplexer connectionMultiplexer, ISerializer<TItem> serializer)
            : base(name, connectionMultiplexer)
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

            var hashSet = value as Memory.HashSetSet<TItem>;
            if (hashSet == null) throw new ArgumentException(@"The specified set type is not supported. Use HashSetSet<TItem> instead.", nameof(value));

            var database = _connectionMultiplexer.GetDatabase();
            var redisKey = GetRedisKey(key);
            if (await database.KeyExistsAsync(redisKey) && !overwrite) return false;

            var transaction = database.CreateTransaction();
            var commandTasks = new List<Task>();
            if (overwrite)
            {
                commandTasks.Add(transaction.KeyDeleteAsync(redisKey));
            }
            foreach (var item in await hashSet.AsEnumerableAsync().ConfigureAwait(false))
            {
                commandTasks.Add(transaction.SetAddAsync(redisKey, _serializer.Serialize(item)));
            }
            var success = await transaction.ExecuteAsync().ConfigureAwait(false);
            await Task.WhenAll(commandTasks).ConfigureAwait(false);
            return success;
        }

        public override async Task<ISet<TItem>> GetValueOrDefaultAsync(TKey key)
        {
            var database = _connectionMultiplexer.GetDatabase();
            if (await database.KeyExistsAsync(GetRedisKey(key)).ConfigureAwait(false))
            {
                return CreateSet(key);
            }

            return null;
        }

        public override Task<bool> TryRemoveAsync(TKey key)
        {
            var database = _connectionMultiplexer.GetDatabase();
            return database.KeyDeleteAsync(GetRedisKey(key));            
        }

        public override Task<bool> ContainsKeyAsync(TKey key)
        {
            var database = _connectionMultiplexer.GetDatabase();
            return database.KeyExistsAsync(GetRedisKey(key));
        }

        protected ISet<TItem> CreateSet(TKey key, bool useScanOnEnumeration = true)
        {
            return new InternalSet(key, GetRedisKey(key), _serializer, _connectionMultiplexer, useScanOnEnumeration);
        }

        private class InternalSet : RedisSet<TItem>
        {
            public InternalSet(TKey key, string setName, ISerializer<TItem> serializer, ConnectionMultiplexer connectionMultiplexer, bool useScanOnEnumeration = true)
                : base(setName, connectionMultiplexer, serializer, useScanOnEnumeration)
            {
                if (key == null) throw new ArgumentNullException(nameof(key));
                Key = key;
            }

            public TKey Key { get; }
        }
    }
}
