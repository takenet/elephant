using Takenet.Elephant.Redis;
using Takenet.Elephant.Redis.Serializers;
using Xunit;

namespace Takenet.Elephant.Tests.Redis
{
    [Collection("Redis")]
    public class RedisStringIntegerStringMapFacts : IntegerStringMapFacts
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
            return new RedisStringMap<int, string>(mapName, "localhost", new StringSerializer());
        }
    }
}
