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
            const string mapName = "guid-item-hash";
            return new RedisHashMap<Guid, Item>(mapName, new ItemDictionaryConverter(), _redisFixture.Connection.Configuration);
        }

        private class ItemDictionaryConverter : IDictionaryConverter<Item>
        {            
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
                    IntegerProperty = (int)(RedisValue) dictionary["IntegerProperty"],
                    StringProperty = (RedisValue) dictionary["StringProperty"],
                    GuidProperty = Guid.Parse((string)(RedisValue)dictionary["GuidProperty"])
                };
            }
        }
    }
}
