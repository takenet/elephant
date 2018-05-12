using System;
using Take.Elephant.Redis;
using Xunit;

namespace Take.Elephant.Tests.Redis
{
    [Trait("Category", nameof(Redis))]
    [Collection(nameof(Redis))]
    public class RedisGuidItemExpirableKeyMapFacts : GuidItemExpirableKeyMapFacts
    {
        private readonly RedisFixture _redisFixture;

        public RedisGuidItemExpirableKeyMapFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;
        }

        public override IExpirableKeyMap<Guid, Item> Create()
        {
            _redisFixture.Server.FlushDatabase();
            const string mapName = "guid-items";
            return new RedisStringMap<Guid, Item>(mapName, _redisFixture.Connection.Configuration, new ItemSerializer());
        }
    }
}
