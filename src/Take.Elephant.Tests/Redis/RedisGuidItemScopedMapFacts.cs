using System;
using System.Threading.Tasks;
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
    public class RedisGuidItemScopedMapFacts : GuidItemScopedMapFacts
    {
        private readonly RedisFixture _redisFixture;
        public const string MapName = "scoped-guid-items";
        private int db = 0;

        public RedisGuidItemScopedMapFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;
        }

        public override IMap<Guid, Item> CreateMap()
        {
            _redisFixture.Server.FlushDatabase(db);
            return new RedisHashMap<Guid, Item>(MapName, new TypeRedisDictionaryConverter<Item>(), _redisFixture.Connection.Configuration, db);            
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
        
        [Fact(Skip = "Atomic add not supported by the current implementation")]
        public override Task AddExistingKeyConcurrentlyReturnsFalse()
        {
            // Not supported by this class
            return base.AddExistingKeyConcurrentlyReturnsFalse();
        }
    }
}