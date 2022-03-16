using System;
using AutoFixture;
using Take.Elephant.Memory;
using Take.Elephant.Redis;
using Xunit;

namespace Take.Elephant.Tests.Redis
{
    [Trait("Category", nameof(Redis))]
    [Collection(nameof(Redis))]
    public class RedisGuidItemSetMapFacts : GuidItemSetMapFacts
    {
        private readonly RedisFixture _redisFixture;

        public RedisGuidItemSetMapFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;
        }

        public string MapName => "guid-items";

        public override IMap<Guid, ISet<Item>> Create()
        {
            var db = 1;
            _redisFixture.Server.FlushDatabase(db);
            var setMap = new RedisSetMap<Guid, Item>(MapName, _redisFixture.Connection.Configuration, new ItemSerializer(), db);
            return setMap;
        }

        public override ISet<Item> CreateValue(Guid key, bool populate)
        {
            var set = new Set<Item>();
            if (populate)
            {
                set.AddAsync(Fixture.Create<Item>()).Wait();
                set.AddAsync(Fixture.Create<Item>()).Wait();
                set.AddAsync(Fixture.Create<Item>()).Wait();
            }
            return set;
        }
    }
}
