using Takenet.Elephant.Redis;
using Xunit;

namespace Takenet.Elephant.Tests.Redis
{
    [Collection("Redis")]
    public class RedisItemSetFacts : ClassSetFacts<Item>
    {
        private readonly RedisFixture _redisFixture;

        public RedisItemSetFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;
        }

   
        public override ISet<Item> Create()
        {
            _redisFixture.Server.FlushDatabase();
            const string setName = "items";
            return new RedisSet<Item>(setName, _redisFixture.Connection.Configuration, new ItemSerializer());
        }
    }
}
