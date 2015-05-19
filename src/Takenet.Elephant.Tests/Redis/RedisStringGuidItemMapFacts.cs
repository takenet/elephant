using System;
using Takenet.Elephant.Redis;
using Xunit;

namespace Takenet.Elephant.Tests.Redis
{
    [Collection("Redis")]
    public class RedisStringGuidItemMapFacts : GuidItemMapFacts
    {
        private readonly RedisFixture _redisFixture;

        public RedisStringGuidItemMapFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;
        }

        public override IMap<Guid, Item> Create()
        {
            _redisFixture.Server.FlushDatabase();
            const string mapName = "guid-items";
            return new RedisStringMap<Guid, Item>(mapName, _redisFixture.Connection.Configuration, new ItemSerializer());
        }
    }
}
