using System;
using AutoFixture;
using Take.Elephant.Memory;
using Take.Elephant.Redis;
using Take.Elephant.Tests.Redis;
using Xunit;

namespace Take.Elephant.Tests.Specialized
{
    [Collection(nameof(Redis))]
    public class RedisMemoryGuidItemOnDemandCacheSetMapFacts : GuidItemOnDemandCacheSetMapFacts
    {
        private readonly RedisFixture _fixture;
        public const string MapName = "guid-items";

        public RedisMemoryGuidItemOnDemandCacheSetMapFacts(RedisFixture fixture)
        {
            _fixture = fixture;
        }

        public override IMap<Guid, ISet<Item>> CreateSource()
        {
            var db = 1;
            _fixture.Server.FlushDatabase(db);
            var setMap = new RedisSetMap<Guid, Item>(MapName, _fixture.Connection.Configuration, new ItemSerializer(), db);
            return setMap;
        }

        public override IMap<Guid, ISet<Item>> CreateCache()
        {
            return new SetMap<Guid, Item>();
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