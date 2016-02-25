using System;
using Takenet.Elephant.Memory;
using Takenet.Elephant.Redis;
using Takenet.Elephant.Redis.Converters;
using Takenet.Elephant.Redis.Serializers;
using Takenet.Elephant.Specialized.Scoping;
using Takenet.Elephant.Tests.Specialized;
using Xunit;

namespace Takenet.Elephant.Tests.Redis
{
    [Collection("Redis")]
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
    }
}