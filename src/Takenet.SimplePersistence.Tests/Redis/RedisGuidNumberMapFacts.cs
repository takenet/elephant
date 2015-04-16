using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Takenet.SimplePersistence.Redis;
using Takenet.SimplePersistence.Redis.Serializers;
using Xunit;

namespace Takenet.SimplePersistence.Tests.Redis
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
