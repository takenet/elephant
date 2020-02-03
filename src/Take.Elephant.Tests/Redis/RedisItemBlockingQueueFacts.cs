using Take.Elephant.Redis;
using Xunit;

namespace Take.Elephant.Tests.Redis
{
    [Trait("Category", nameof(Redis))]
    [Collection(nameof(Redis))]
    public class RedisItemBlockingQueueFacts : ItemBlockingQueueFacts
    {
        private readonly RedisFixture _redisFixture;

        public RedisItemBlockingQueueFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;            
        }

        public override IQueue<Item> Create()
        {
            _redisFixture.Server.FlushDatabase();
            const string setName = "items";            
            return new RedisBlockingQueue<Item>(setName, _redisFixture.Connection.Configuration, new ItemSerializer());
        }
    }
}
