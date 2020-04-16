using System;
using Take.Elephant.Memory;
using Take.Elephant.Redis;
using Take.Elephant.Redis.Converters;
using Take.Elephant.Tests.Redis;
using Xunit;

namespace Take.Elephant.Tests.Specialized
{
    [Collection(nameof(Redis))]
    [Trait("Category", nameof(Redis))]
    public class RedisMemoryGuidItemOnDemandCacheMapFacts : GuidItemOnDemandCacheMapFacts
    {
        private readonly RedisFixture _fixture;

        public RedisMemoryGuidItemOnDemandCacheMapFacts(RedisFixture fixture)
        {
            _fixture = fixture;
        }
        
        public override IMap<Guid, Item> CreateSource()
        {
            int db = 0;
            _fixture.Server.FlushDatabase(db);
            const string mapName = "guid-item-hash";
            return new RedisHashMap<Guid, Item>(mapName, new TypeRedisDictionaryConverter<Item>(), _fixture.Connection.Configuration, db);
        }

        public override IMap<Guid, Item> CreateCache()
        {
            return new Map<Guid, Item>();
        }
    }
}