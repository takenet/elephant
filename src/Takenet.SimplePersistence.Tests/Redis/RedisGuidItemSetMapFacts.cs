using System;

using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NFluent;
using Ploeh.AutoFixture;
using Takenet.SimplePersistence.Memory;
using Takenet.SimplePersistence.Redis;
using Takenet.SimplePersistence.Redis.Serializers;
using Xunit;

namespace Takenet.SimplePersistence.Tests.Redis
{
    [Collection("Redis")]
    public class RedisGuidItemSetMapFacts : GuidItemSetMapFacts
    {
        private readonly RedisFixture _redisFixture;
        public const string MapName = "guid-items";

        public RedisGuidItemSetMapFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;
        }

        public override IMap<Guid, ISet<Item>> Create()
        {            
            _redisFixture.Server.FlushDatabase();            
            var setMap = new RedisSetMap<Guid, Item>(MapName, _redisFixture.Connection.Configuration, new ItemSerializer());
            return setMap;
        }

        public override ISet<Item> CreateValue(Guid key)
        {
            var set = new Set<Item>();
            set.AddAsync(Fixture.Create<Item>()).Wait();
            set.AddAsync(Fixture.Create<Item>()).Wait();
            set.AddAsync(Fixture.Create<Item>()).Wait();
            return set;
        }
    }
}
