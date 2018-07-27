using System;
using Take.Elephant.Redis;
using Take.Elephant.Redis.Serializers;
using Xunit;

namespace Take.Elephant.Tests.Redis
{
    [Trait("Category", nameof(Redis))]
    [Collection(nameof(Redis))]
    public class RedisGuidQueueFacts : GuidItemQueueFacts
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
