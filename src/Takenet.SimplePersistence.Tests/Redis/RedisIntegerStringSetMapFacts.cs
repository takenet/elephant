using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Ploeh.AutoFixture;
using Takenet.SimplePersistence.Redis;
using Takenet.SimplePersistence.Redis.Serializers;
using Xunit;

namespace Takenet.SimplePersistence.Tests.Redis
{
    [Collection("Redis")]
    public class RedisIntegerStringSetMapFacts : IntegerStringSetMapFacts, IClassFixture<RedisFixture>
    {
        private readonly RedisFixture _redisFixture;
        public const string MapName = "integer-strings";

        public RedisIntegerStringSetMapFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;
        }

        public override IMap<int, ISet<string>> Create()
        {            
            _redisFixture.Server.FlushDatabase();            
            var setMap = new RedisSetMap<int, string>(MapName, _redisFixture.Connection.Configuration, new StringSerializer());
            return setMap;
        }

        public override ISet<string> CreateValue(int key)
        {
            return new RedisSetMap<int, string>(MapName, _redisFixture.Connection, new StringSerializer()).CreateSet(key);
        }
    }
}
