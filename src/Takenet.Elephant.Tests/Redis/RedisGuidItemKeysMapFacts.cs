using System;
using Takenet.Elephant.Redis;
using Takenet.Elephant.Redis.Converters;
using Xunit;

namespace Takenet.Elephant.Tests.Redis
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
