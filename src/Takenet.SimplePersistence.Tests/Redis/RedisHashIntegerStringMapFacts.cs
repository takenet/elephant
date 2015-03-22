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
    public class RedisHashIntegerStringMapFacts : IntegerStringMapFacts, IClassFixture<RedisFixture>
    {
        private readonly RedisFixture _redisFixture;

        public RedisHashIntegerStringMapFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;
        }

        public override IMap<int, string> Create()
        {
            _redisFixture.Server.FlushDatabase();
            const string mapName = "integer-object-hash";
            return new RedisHashMap<int, string>(mapName, new StringDictionaryConverter(), "localhost");
        }

        private class StringDictionaryConverter : IDictionaryConverter<string>
        {
            public string FromDictionary(IDictionary<string, object> dictionary)
            {
                return (RedisValue)dictionary["value"];
            }

            public IDictionary<string, object> ToDictionary(string value)
            {
                return new Dictionary<string, object>()
                {
                    {"value", value}
                };
            }
        }
    }
}
