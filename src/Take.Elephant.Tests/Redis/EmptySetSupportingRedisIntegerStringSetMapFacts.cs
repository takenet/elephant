using System;
using System.Threading.Tasks;
using AutoFixture;
using Take.Elephant.Memory;
using Take.Elephant.Redis;
using Take.Elephant.Redis.Serializers;
using Xunit;

namespace Take.Elephant.Tests.Redis
{
    [Trait("Category", nameof(Redis))]
    [Collection(nameof(Redis))]
    public class EmptySetSupportingRedisIntegerStringSetMapFacts : IntegerStringSetMapFacts
    {
        private readonly RedisFixture _redisFixture;

        public EmptySetSupportingRedisIntegerStringSetMapFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;
        }

        public string MapName => "integer-strings";
   
        public override IMap<int, ISet<string>> Create()
        {
            var db = 1;
            _redisFixture.Server.FlushDatabase(db);
            var setMap = new RedisSetMap<int, string>(MapName, _redisFixture.Connection.Configuration, new StringSerializer(), db, supportEmptySets: true);
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

        [Fact]
        public async Task AccessingEmptyKeyTwiceAfterSettingMainKeyExpirationHitsTheDatabaseTwice()
        {
            // Arrange
            var ttl = TimeSpan.FromMilliseconds(50);
            var map = Create() as RedisSetMap<int, string>;
            var key = CreateKey();
            await map.AddItemAsync(key, "foo");

            // Act
            await map.SetRelativeKeyExpirationAsync(key, ttl);
            await Task.Delay(ttl * 2);
            var actual = await map.GetValueOrDefaultAsync(key);

            // Assert
            AssertIsDefault(actual);
        }
    }
}
