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
    public class RedisStringSetFacts : StringSetFacts, IClassFixture<RedisFixture>
    {
        private readonly RedisFixture _redisFixture;

        public RedisStringSetFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;            
        }

        public override ISet<string> Create()
        {
            _redisFixture.Server.FlushDatabase();
            const string setName = "strings";            
            return new RedisSet<string>(setName, new StringSerializer(), _redisFixture.Connection.Configuration);
        }
    }
}
