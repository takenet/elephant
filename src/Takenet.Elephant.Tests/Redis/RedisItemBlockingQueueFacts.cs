using Takenet.Elephant.Redis;
using Xunit;

namespace Takenet.Elephant.Tests.Redis
{
    [Collection("Redis")]
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
            return new RedisQueue<Item>(setName, _redisFixture.Connection.Configuration, new ItemSerializer());
        }
    }
}
