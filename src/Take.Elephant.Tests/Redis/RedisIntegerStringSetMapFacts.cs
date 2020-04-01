using AutoFixture;
using Take.Elephant.Memory;
using Take.Elephant.Redis;
using Take.Elephant.Redis.Serializers;
using Xunit;

namespace Take.Elephant.Tests.Redis
{
    [Trait("Category", nameof(Redis))]
    [Collection(nameof(Redis))]
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
            var db = 1;
            _redisFixture.Server.FlushDatabase(db);            
            var setMap = new RedisSetMap<int, string>(MapName, _redisFixture.Connection.Configuration, new StringSerializer(), db);
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
