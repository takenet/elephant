using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Takenet.SimplePersistence.Redis;
using Takenet.SimplePersistence.Redis.Serializers;
using Xunit;

namespace Takenet.SimplePersistence.Tests.Redis
{
    [Collection("Redis")]
    public class RedisStringGuidItemMapFacts : GuidItemMapFacts, IClassFixture<RedisFixture>
    {
        private readonly RedisFixture _redisFixture;

        public RedisStringGuidItemMapFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;
        }

        public override IMap<Guid, Item> Create()
        {
            _redisFixture.Server.FlushDatabase();
            const string mapName = "guid-items";
            return new RedisStringMap<Guid, Item>(mapName, new ItemSerializer(), "localhost");
        }
    }

    public class ItemSerializer : ISerializer<Item>
    {
        public string Serialize(Item value)
        {
            return value.ToString();
        }

        public Item Deserialize(string value)
        {
            return Item.Parse(value);
        }
    }
}
