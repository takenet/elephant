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
    public class RedisStringIntegerStringMapFacts : IntegerStringMapFacts, IClassFixture<RedisFixture>
    {
        private readonly RedisFixture _redisFixture;

        public RedisStringIntegerStringMapFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;
        }

        public override IMap<int, string> Create()
        {
            _redisFixture.Server.FlushDatabase();
            const string mapName = "integer-object";
            return new RedisStringMap<int, string>(mapName, new StringSerializer(), "localhost");
        }
    }
}
