using Takenet.Elephant.Redis;
using Takenet.Elephant.Redis.Converters;
using Xunit;

namespace Takenet.Elephant.Tests.Redis
{
    [Trait("Category", nameof(Redis))]
    [Collection(nameof(Redis))]
    public class RedisHashIntegerStringMapFacts : IntegerStringMapFacts
    {
        private readonly RedisFixture _redisFixture;

        public RedisHashIntegerStringMapFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;
        }

        public override IMap<int, string> Create()
        {
            _redisFixture.Server.FlushDatabase();
            const string mapName = "integer-object-hash";
            return new RedisHashMap<int, string>(mapName, new ValueRedisDictionaryConverter<string>(), "localhost");
        }
    }
}
