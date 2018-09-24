using System;
using Take.Elephant.Redis;
using Take.Elephant.Redis.Serializers;
using Xunit;

namespace Take.Elephant.Tests.Redis
{
    [Trait("Category", nameof(Redis))]
    [Collection(nameof(Redis))]
    public class RedisGuidItemSortedSetFacts : GuidItemSortedSetFacts
    {
        private readonly RedisFixture _redisFixture;

        public RedisGuidItemSortedSetFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;
        }

        public override ISortedSet<Guid> Create()
        {
            var db = 1;
            _redisFixture.Server.FlushDatabase(db);
            const string setName = "guids";
            return new RedisSortedSet<Guid>(setName, _redisFixture.Connection.Configuration, new ValueSerializer<Guid>(), db);
        }
    }
}