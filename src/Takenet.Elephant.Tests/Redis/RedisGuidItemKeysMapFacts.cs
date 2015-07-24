using System;
using System.Data;
using System.Reflection;
using Takenet.Elephant.Redis;
using Takenet.Elephant.Redis.Converters;
using Takenet.Elephant.Sql;
using Takenet.Elephant.Sql.Mapping;
using Takenet.Elephant.Tests.Sql;
using Xunit;

namespace Takenet.Elephant.Tests.Redis
{
    [Collection("Redis")]
    public class RedisGuidItemKeysMapFacts : GuidItemKeysMapFacts
    {
        private readonly RedisFixture _redisFixture;

        public RedisGuidItemKeysMapFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;
        }

        public override IKeysMap<Guid, Item> Create()
        {
            _redisFixture.Server.FlushDatabase();
            const string mapName = "guid-item-hash";
            return new RedisHashMap<Guid, Item>(mapName, new TypeRedisDictionaryConverter<Item>(), _redisFixture.Connection.Configuration);
        }
    }
}
