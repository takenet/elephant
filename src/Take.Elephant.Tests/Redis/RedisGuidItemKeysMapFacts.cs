using System;
using Take.Elephant.Redis;
using Take.Elephant.Redis.Converters;
using Xunit;

namespace Take.Elephant.Tests.Redis
{
    [Trait("Category", nameof(Redis))]
    [Collection(nameof(Redis))]
    public class RedisGuidItemKeysMapFacts : GuidItemKeysMapFacts
    {
        private readonly RedisFixture _redisFixture;

        public RedisGuidItemKeysMapFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;
        }

        public override IKeysMap<Guid, Item> Create()
        {
            _redisFixture.Server.FlushDatabase();
            const string mapName = "guid-item-hash";
            return new RedisHashMap<Guid, Item>(mapName, new TypeRedisDictionaryConverter<Item>(), _redisFixture.Connection.Configuration);
        }
    }
}
