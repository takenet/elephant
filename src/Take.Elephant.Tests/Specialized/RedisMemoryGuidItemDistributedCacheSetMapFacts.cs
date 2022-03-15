using System;
using System.Threading.Tasks;
using AutoFixture;
using Take.Elephant.Memory;
using Take.Elephant.Redis;
using Take.Elephant.Specialized.Cache;
using Take.Elephant.Tests.Redis;
using Xunit;

namespace Take.Elephant.Tests.Specialized
{
    [Collection("Redis")]
    [Trait("Category", nameof(Redis))]
    public class RedisMemoryGuidItemDistributedCacheSetMapFacts : GuidItemDistributedCacheSetMapFacts
    {
        private readonly RedisFixture _redisFixture;

        public RedisMemoryGuidItemDistributedCacheSetMapFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;
        }        
        
        public override IMap<Guid, ISet<Item>> CreateSource()
        {
            var db = 1;
            _redisFixture.Server.FlushDatabase(db);            
            var setMap = new RedisSetMap<Guid, Item>("guid-items", _redisFixture.Connection.Configuration, new ItemSerializer(), db);
            return setMap;
        }
        
        public override IMap<Guid, ISet<Item>> CreateCache()
        {
            return new SetMap<Guid, Item>();
        }

        public override IBus<string, SynchronizationEvent<Guid>> CreateSynchronizationBus()
        {
            return new Bus<string, SynchronizationEvent<Guid>>();
        }

        public override ISet<Item> CreateValue(Guid key, bool populate)
        {
            var set = new Set<Item>();
            if (populate)
            {
                set.AddAsync(Fixture.Create<Item>()).Wait();
                set.AddAsync(Fixture.Create<Item>()).Wait();
                set.AddAsync(Fixture.Create<Item>()).Wait();
            }
            return set;
        }
        
        [Fact(Skip = "Atomic add not supported by the current implementation")]
        public override Task AddExistingKeyConcurrentlyReturnsFalse()
        {
            // Not supported by this class
            return base.AddExistingKeyConcurrentlyReturnsFalse();
        }
    }
}