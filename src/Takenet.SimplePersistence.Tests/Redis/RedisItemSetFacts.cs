using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Takenet.SimplePersistence.Redis;
using Xunit;

namespace Takenet.SimplePersistence.Tests.Redis
{
    [Collection("Redis")]
    public class RedisItemSetFacts : ClassSetFacts<Item>
    {
        private readonly RedisFixture _redisFixture;

        public RedisItemSetFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;
        }

   
        public override ISet<Item> Create()
        {
            _redisFixture.Server.FlushDatabase();
            const string setName = "items";
            return new RedisSet<Item>(setName, new ItemSerializer(), _redisFixture.Connection.Configuration);
        }
    }
}
