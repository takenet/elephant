using System;
using System.Threading.Tasks;
using Take.Elephant.Redis;
using Take.Elephant.Redis.Converters;
using Xunit;

namespace Take.Elephant.Tests.Redis
{
    [Trait("Category", nameof(Redis))]
    [Collection(nameof(Redis))]
    public class RedisHashGuidItemMapFacts : GuidItemMapFacts
    {
        private readonly RedisFixture _redisFixture;

        public RedisHashGuidItemMapFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;
        }

        public override IMap<Guid, Item> Create()
        {
            int db = 0;
            _redisFixture.Server.FlushDatabase(db);
            const string mapName = "guid-item-hash";
            return new RedisHashMap<Guid, Item>(mapName, new TypeRedisDictionaryConverter<Item>(), _redisFixture.Connection.Configuration, db);
        }
        
        public override Task AddExistingKeyConcurrentlyReturnsFalse()
        {
            // Not supported by this class
            return Task.CompletedTask;
        }
    }
}
