using Take.Elephant.Redis;
using Take.Elephant.Redis.Serializers;
using Xunit;

namespace Take.Elephant.Tests.Redis
{
    [Trait("Category", nameof(Redis))]
    [Collection(nameof(Redis))]
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