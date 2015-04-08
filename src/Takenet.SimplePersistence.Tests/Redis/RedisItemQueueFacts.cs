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
    public class RedisItemQueueFacts : ItemQueueFacts
    {
        private readonly RedisFixture _redisFixture;

        public RedisItemQueueFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;            
        }

        public override IQueue<Item> Create()
        {
            _redisFixture.Server.FlushDatabase();
            const string setName = "items";            
            return new RedisQueue<Item>(setName, _redisFixture.Connection.Configuration, new ItemSerializer());
        }
    }
}
