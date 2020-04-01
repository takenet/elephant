using System;
using AutoFixture;
using Take.Elephant.Memory;
using Take.Elephant.Redis;
using Take.Elephant.Redis.Converters;
using Take.Elephant.Redis.Serializers;
using Take.Elephant.Specialized.Scoping;
using Take.Elephant.Tests.Specialized;
using Xunit;

namespace Take.Elephant.Tests.Redis
{
    [Trait("Category", nameof(Redis))]
    [Collection(nameof(Redis))]
    public class RedisGuidItemScopedSetMapFacts : GuidItemScopedSetMapFacts
    {
        private readonly RedisFixture _redisFixture;
        public const string MapName = "scoped-guid-items";
        private int db = 0;

        public RedisGuidItemScopedSetMapFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;
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

        public override IMap<Guid, ISet<Item>> CreateMap()
        {
            var db = 1;
            _redisFixture.Server.FlushDatabase(db);
            var setMap = new RedisSetMap<Guid, Item>(MapName, _redisFixture.Connection.Configuration, new ItemSerializer(), db);
            return setMap;
        }

        public override ISetMap<string, IdentifierKey> CreateKeysSetMap()
        {
            var setMap = new RedisSetMap<string, IdentifierKey>("scope", _redisFixture.Connection.Configuration, new ValueSerializer<IdentifierKey>(), db);
            return setMap;            
        }

        public override ISerializer<Guid> CreateKeySerializer()
        {
            return new GuidSerializer();
        }

        public override bool RemoveOnEmptySet() => true;
    }
}