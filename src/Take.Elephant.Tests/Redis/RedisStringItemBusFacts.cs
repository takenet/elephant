using Take.Elephant.Redis;
using Xunit;

namespace Take.Elephant.Tests.Redis
{
    [Trait("Category", nameof(Redis))]
    [Collection(nameof(Redis))]
    public class RedisStringItemBusFacts : StringItemBusFacts
    {
        private readonly RedisFixture _redisFixture;

        public RedisStringItemBusFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;
        }

        public override IBus<string, Item> Create()
        {
            var db = 1;
            _redisFixture.Server.FlushDatabase(db);
            return new RedisBus<string, Item>("items", _redisFixture.Connection.Configuration, new ItemSerializer(), db: db);
        }
    }
}