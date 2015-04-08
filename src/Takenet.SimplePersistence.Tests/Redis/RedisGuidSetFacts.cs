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
    public class RedisGuidSetFacts : GuidSetFacts
    {
        private readonly RedisFixture _redisFixture;

        public RedisGuidSetFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;            
        }

        public override ISet<Guid> Create()
        {
            _redisFixture.Server.FlushDatabase();
            const string setName = "guids";            
            return new RedisSet<Guid>(setName, _redisFixture.Connection.Configuration, new GuidSerializer());
        }
    }
}
