using System;
using Takenet.Elephant.Redis;
using Takenet.Elephant.Redis.Serializers;
using Xunit;

namespace Takenet.Elephant.Tests.Redis
{
    [Collection("Redis")]
    public class RedisGuidSetFacts : GuidSetFacts
    {
        private readonly RedisFixture _redisFixture;

        public RedisGuidSetFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;            
        }

        public override ISet<Guid> Create()
        {
            var db = 1;
            _redisFixture.Server.FlushDatabase(db);
            const string setName = "guids";            
            return new RedisSet<Guid>(setName, _redisFixture.Connection.Configuration, new ValueSerializer<Guid>(), db);
        }
    }
}
