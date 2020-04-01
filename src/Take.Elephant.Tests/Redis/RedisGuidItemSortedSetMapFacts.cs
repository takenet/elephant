using AutoFixture;
using System;
using Take.Elephant.Memory;
using Take.Elephant.Redis;
using Xunit;

namespace Take.Elephant.Tests.Redis
{
    [Trait("Category", nameof(Redis))]
    [Collection(nameof(Redis))]
    public class RedisGuidItemSortedSetMapFacts : GuidItemSortedSetMapFacts
    {
        private readonly RedisFixture _redisFixture;
        private readonly int _db;
        public const string _setName = "guid-set-items";

        public RedisGuidItemSortedSetMapFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;
            _db = 1;
        }

        public override IMap<Guid, ISortedSet<Item>> Create()
        {
            _redisFixture.Server.FlushDatabase(_db); ;
            return new RedisSortedSetMap<Guid, Item>(_setName, _redisFixture.Connection.Configuration, new ItemSerializer(), _db);
        }

        public override ISortedSet<Item> CreateValue(Guid key, bool populate)
        {
            var sortedSet = new SortedSet<Item>();
            if (populate)
            {
                sortedSet.AddAsync(Fixture.Create<Item>(), 1).Wait();
                sortedSet.AddAsync(Fixture.Create<Item>(), 1).Wait();
                sortedSet.AddAsync(Fixture.Create<Item>(), 1).Wait();
            }
            return sortedSet;
        }
    }
}