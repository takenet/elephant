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
    public class RedisGuidItemQueueMapFacts : GuidItemQueueMapFacts
    {
        private readonly RedisFixture _redisFixture;
        public const string MapName = "guid-items";

        public RedisGuidItemQueueMapFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;
        }

        public override IMap<Guid, IQueue<Item>> Create()
        {
            _redisFixture.Server.FlushDatabase();
            var setMap = new RedisQueueMap<Guid, Item>(MapName, _redisFixture.Connection.Configuration, new ItemSerializer());
            return setMap;
        }

        public override IQueue<Item> CreateValue(Guid key)
        {
            var set = new Queue<Item>();
            set.EnqueueAsync(Fixture.Create<Item>()).Wait();
            set.EnqueueAsync(Fixture.Create<Item>()).Wait();
            set.EnqueueAsync(Fixture.Create<Item>()).Wait();
            return set;
        }
    }
}
