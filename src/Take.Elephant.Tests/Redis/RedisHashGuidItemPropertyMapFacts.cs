using System;
using Take.Elephant.Redis;
using Take.Elephant.Redis.Converters;
using Xunit;

namespace Take.Elephant.Tests.Redis
{
    [Trait("Category", nameof(Redis))]
    [Collection(nameof(Redis))]
    public class RedisHashGuidItemPropertyMapFacts : GuidItemPropertyMapFacts
    {
        private readonly RedisFixture _redisFixture;

        public RedisHashGuidItemPropertyMapFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;
        }

        public override IPropertyMap<Guid, Item> Create()
        {
            _redisFixture.Server.FlushDatabase();
            const string mapName = "guid-item-hash";
            return new RedisHashMap<Guid, Item>(mapName, new TypeRedisDictionaryConverter<Item>(), _redisFixture.Connection.Configuration);
        }
    }
}
