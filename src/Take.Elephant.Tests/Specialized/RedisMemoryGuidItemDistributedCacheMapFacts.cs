using System;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Take.Elephant.Memory;
using Take.Elephant.Redis;
using Take.Elephant.Redis.Serializers;
using Take.Elephant.Specialized.Cache;
using Take.Elephant.Tests.Redis;
using Xunit;

namespace Take.Elephant.Tests.Specialized
{
    [Collection("Redis")]
    [Trait("Category", nameof(Redis))]
    public class RedisMemoryGuidItemDistributedCacheMapFacts : GuidItemDistributedCacheMapFacts
    {
        private readonly RedisFixture _redisFixture;

        public RedisMemoryGuidItemDistributedCacheMapFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;
        }

        public override IMap<Guid, Item> CreateSource()
        {
            _redisFixture.Server.FlushDatabase();
            const string mapName = "guid-items";
            return new RedisStringMap<Guid, Item>(mapName, _redisFixture.Connection.Configuration,
                new ItemSerializer());
        }

        public override IMap<Guid, Item> CreateCache()
        {
            return new Map<Guid, Item>();
        }

        public override IBus<string, SynchronizationEvent<Guid>> CreateSynchronizationBus()
        {
            return new RedisBus<string, SynchronizationEvent<Guid>>(
                "guid-items", _redisFixture.Connection.Configuration, new SynchronizationEventJsonSerializer());
        }
        
        [Fact(Skip = "Atomic add not supported by the current implementation")]
        public override Task AddExistingKeyConcurrentlyReturnsFalse()
        {
            // Not supported by this class
            return base.AddExistingKeyConcurrentlyReturnsFalse();
        }
    }
    
    public class SynchronizationEventJsonSerializer : ISerializer<SynchronizationEvent<Guid>>
    {
        public SynchronizationEvent<Guid> Deserialize(string value)
        {
            return JsonConvert.DeserializeObject<SynchronizationEvent<Guid>>(value);
        }

        public string Serialize(SynchronizationEvent<Guid> value)
        {
            return JsonConvert.SerializeObject(value);
        }
    }
}