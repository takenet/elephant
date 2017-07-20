using System;
using Takenet.Elephant.Redis;
using Takenet.Elephant.Redis.Serializers;
using Xunit;

namespace Takenet.Elephant.Tests.Redis
{
    [Trait("Category", nameof(Redis))]
    [Collection(nameof(Redis))]
    public class RedisGuidQueueFacts : GuidQueueFacts
    {
        private readonly RedisFixture _redisFixture;

        public RedisGuidQueueFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;            
        }

        public override IQueue<Guid> Create()
        {
            int db = 2;
            _redisFixture.Server.FlushDatabase(db);
            const string setName = "guids";            
            return new RedisQueue<Guid>(setName, _redisFixture.Connection.Configuration, new ValueSerializer<Guid>(), db);
        }
    }
}
