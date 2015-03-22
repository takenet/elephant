using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;
using Takenet.SimplePersistence.Redis;
using Takenet.SimplePersistence.Redis.Serializers;
using Xunit;

namespace Takenet.SimplePersistence.Tests.Redis
{
    [Collection("Redis")]
    public class RedisHashGuidItemMapFacts : GuidItemMapFacts, IClassFixture<RedisFixture>
    {
        private readonly RedisFixture _redisFixture;

        public RedisHashGuidItemMapFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;
        }

        public override IMap<Guid, Item> Create()
        {
            _redisFixture.Server.FlushDatabase();
            const string mapName = "integer-object-hash";
            return new RedisHashMap<Guid, Item>(mapName, new ItemDictionaryConverter(), "localhost");
        }

        private class ItemDictionaryConverter : IDictionaryConverter<Item>
        {            
            public IDictionary<string, object> ToDictionary(Item value)
            {
                return new Dictionary<string, object>()
                {
                    {"integerProperty", value.IntegerProperty},
                    {"stringProperty", value.StringProperty}
                };
            }         

            public Item FromDictionary(IDictionary<string, object> dictionary)
            {
                return new Item()
                {
                    IntegerProperty = (int)(RedisValue) dictionary["integerProperty"],
                    StringProperty = (RedisValue) dictionary["stringProperty"],
                };
            }
        }
    }
}
