using System;
using Ploeh.AutoFixture;
using Takenet.Elephant.Memory;
using Takenet.Elephant.Redis;
using Xunit;

namespace Takenet.Elephant.Tests.Redis
{
    [Collection("Redis")]
    public class RedisGuidItemQueueMapFacts : GuidItemQueueMapFacts
    {
        private readonly RedisFixture _redisFixture;
        public const string MapName = "guid-items-queue";

        public RedisGuidItemQueueMapFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;
        }

        public override IMap<Guid, IQueue<Item>> Create()
        {
            int db = 2;
            _redisFixture.Server.FlushDatabase(db);
            var setMap = new RedisQueueMap<Guid, Item>(MapName, _redisFixture.Connection.Configuration, new ItemSerializer(), db);
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
