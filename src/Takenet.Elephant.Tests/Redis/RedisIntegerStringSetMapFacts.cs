using Ploeh.AutoFixture;
using Takenet.Elephant.Memory;
using Takenet.Elephant.Redis;
using Takenet.Elephant.Redis.Serializers;
using Xunit;

namespace Takenet.Elephant.Tests.Redis
{
    [Collection("Redis")]
    public class RedisIntegerStringSetMapFacts : IntegerStringSetMapFacts
    {
        private readonly RedisFixture _redisFixture;
        public const string MapName = "integer-strings";

        public RedisIntegerStringSetMapFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;
        }

        public override IMap<int, ISet<string>> Create()
        {            
            _redisFixture.Server.FlushDatabase();            
            var setMap = new RedisSetMap<int, string>(MapName, _redisFixture.Connection.Configuration, new StringSerializer());
            return setMap;
        }

        public override ISet<string> CreateValue(int key, bool populate)
        {
            var set = new Set<string>();
            if (populate)
            {
                set.AddAsync(Fixture.Create<string>()).Wait();
                set.AddAsync(Fixture.Create<string>()).Wait();
                set.AddAsync(Fixture.Create<string>()).Wait();
            }
            return set;
        }
    }
}
