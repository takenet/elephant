using Take.Elephant.Redis;
using Xunit;

namespace Take.Elephant.Tests.Redis
{
    [Trait("Category", nameof(Redis))]
    [Collection(nameof(Redis))]
    public class RedisItemSetFacts : ClassSetFacts<Item>
    {
        private readonly RedisFixture _redisFixture;

        public RedisItemSetFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;
        }

   
        public override ISet<Item> Create()
        {
            var db = 1;
            _redisFixture.Server.FlushDatabase(db);
            const string setName = "items";
            return new RedisSet<Item>(setName, _redisFixture.Connection.Configuration, new ItemSerializer(), db);
        }
    }
}
