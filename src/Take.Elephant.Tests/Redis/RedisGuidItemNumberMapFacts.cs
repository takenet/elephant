using System;
using Take.Elephant.Redis;
using Take.Elephant.Redis.Serializers;
using Xunit;

namespace Take.Elephant.Tests.Redis
{
    [Trait("Category", nameof(Redis))]
    [Collection(nameof(Redis))]
    public class RedisGuidItemNumberMapFacts : GuidItemNumberMapFacts
    {
        private readonly RedisFixture _redisFixture;

        public RedisGuidItemNumberMapFacts(RedisFixture redisFixture)
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
