using System;
using System.Collections.Generic;
using StackExchange.Redis;
using Takenet.Elephant.Redis;
using Takenet.Elephant.Redis.Converters;
using Xunit;

namespace Takenet.Elephant.Tests.Redis
{
    [Trait("Category", nameof(Redis))]
    [Collection(nameof(Redis))]
    public class RedisHashGuidItemMapFacts : GuidItemMapFacts
    {
        private readonly RedisFixture _redisFixture;

        public RedisHashGuidItemMapFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;
        }

        public override IMap<Guid, Item> Create()
        {
            int db = 0;
            _redisFixture.Server.FlushDatabase(db);
            const string mapName = "guid-item-hash";
            return new RedisHashMap<Guid, Item>(mapName, new TypeRedisDictionaryConverter<Item>(), _redisFixture.Connection.Configuration, db);
        }
    }

    public class ItemDictionaryConverter : IDictionaryConverter<Item>
    {
        public IEnumerable<string> Properties => new[] { "IntegerProperty", "StringProperty", "GuidProperty" };


        public IDictionary<string, object> ToDictionary(Item value)
        {
            return new Dictionary<string, object>()
            {
                {nameof(value.IntegerProperty), value.IntegerProperty},
                {nameof(value.StringProperty), value.StringProperty},
                {nameof(value.GuidProperty), value.GuidProperty.ToString()}
            };
        }

        public Item FromDictionary(IDictionary<string, object> dictionary)
        {
            return new Item()
            {
                IntegerProperty = (int)(RedisValue)dictionary["IntegerProperty"],
                StringProperty = (RedisValue)dictionary["StringProperty"],
                GuidProperty = Guid.Parse((string)(RedisValue)dictionary["GuidProperty"])
            };
        }
    }
}
