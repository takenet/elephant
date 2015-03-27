using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;
using StackExchange.Redis.KeyspaceIsolation;
using Takenet.SimplePersistence.Memory;

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
            if (value is Set)
            {
                var set = (Set)value;
                return key.Equals(set.Key);
            }

            if (value is HashSet<TItem>)
            {
                var hashSet = (HashSet<TItem>) value;
                var set = CreateSet(key);

                foreach (var item in await hashSet.AsEnumerableAsync())
                {
                    await set.AddAsync(item).ConfigureAwait(false);
                }                
            }
   
            return false;
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

        public ISet<TItem> CreateSet(TKey key, bool useScanOnEnumeration = true)
        {
            return new Set(key, GetRedisKey(key), _serializer, _connectionMultiplexer, useScanOnEnumeration);
        }

        private class Set : RedisSet<TItem>
        {
            public Set(TKey key, string setName, ISerializer<TItem> serializer, ConnectionMultiplexer connectionMultiplexer, bool useScanOnEnumeration = true) 
                : base(setName, serializer, connectionMultiplexer, useScanOnEnumeration)
            {
                if (key == null) throw new ArgumentNullException(nameof(key));
                Key = key;
            }

            public TKey Key { get; }
        }

    }
}
