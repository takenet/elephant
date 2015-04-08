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
    public class RedisGuidQueueFacts : GuidQueueFacts
    {
        private readonly RedisFixture _redisFixture;

        public RedisGuidQueueFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;            
        }

        public override IQueue<Guid> Create()
        {
            _redisFixture.Server.FlushDatabase();
            const string setName = "guids";            
            return new RedisQueue<Guid>(setName, _redisFixture.Connection.Configuration, new GuidSerializer());
        }
    }
}
