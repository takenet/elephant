using Ploeh.AutoFixture;
using Takenet.Elephant.Memory;
using Takenet.Elephant.Redis;
using Takenet.Elephant.Redis.Serializers;
using Xunit;

namespace Takenet.Elephant.Tests.Redis
{
    [Trait("Category", nameof(Redis))]
    [Collection(nameof(Redis))]
    public class RedisIntegerStringQueueMapFacts : IntegerStringQueueMapFacts
    {
        private readonly RedisFixture _redisFixture;
        public const string MapName = "integer-strings";

        public RedisIntegerStringQueueMapFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;
        }

        public override IMap<int, IQueue<string>> Create()
        {
            int db = 2;
            _redisFixture.Server.FlushDatabase(db);            
            var setMap = new RedisQueueMap<int, string>(MapName, _redisFixture.Connection.Configuration, new StringSerializer(), db);
            return setMap;
        }

        public override IQueue<string> CreateValue(int key)
        {
            var queue = new Queue<string>();
            queue.EnqueueAsync(Fixture.Create<string>()).Wait();
            queue.EnqueueAsync(Fixture.Create<string>()).Wait();
            queue.EnqueueAsync(Fixture.Create<string>()).Wait();
            return queue;
        }
    }
}
