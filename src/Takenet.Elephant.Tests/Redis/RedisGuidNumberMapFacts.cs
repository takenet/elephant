using System;
using Takenet.Elephant.Redis;
using Takenet.Elephant.Redis.Serializers;
using Xunit;

namespace Takenet.Elephant.Tests.Redis
{
    [Collection("Redis")]
    public class RedisGuidNumberMapFacts : GuidNumberMapFacts
    {
        private readonly RedisFixture _redisFixture;

        public RedisGuidNumberMapFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;
        }

        public override INumberMap<Guid> Create()
        {
            _redisFixture.Server.FlushDatabase();
            const string mapName = "guid-numbers";
            var setMap = new RedisNumberMap<Guid>(mapName, _redisFixture.Connection.Configuration, new ValueSerializer<long>());
            return setMap;
        }
    }
}
