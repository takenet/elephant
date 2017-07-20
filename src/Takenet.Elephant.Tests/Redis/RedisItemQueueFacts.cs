using Takenet.Elephant.Redis;
using Xunit;

namespace Takenet.Elephant.Tests.Redis
{
    [Trait("Category", nameof(Redis))]
    [Collection(nameof(Redis))]
    public class RedisItemQueueFacts : ItemQueueFacts
    {
        private readonly RedisFixture _redisFixture;

        public RedisItemQueueFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;            
        }

        public override IQueue<Item> Create()
        {
            _redisFixture.Server.FlushDatabase();
            const string setName = "items";            
            return new RedisQueue<Item>(setName, _redisFixture.Connection.Configuration, new ItemSerializer());
        }
    }
}
