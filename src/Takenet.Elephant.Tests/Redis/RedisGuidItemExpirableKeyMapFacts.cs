using System;
using Takenet.Elephant.Redis;
using Xunit;

namespace Takenet.Elephant.Tests.Redis
{
    [Collection("Redis")]
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
