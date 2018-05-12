using Xunit;

namespace Take.Elephant.Tests.Redis
{
    [CollectionDefinition("Redis")]
    public class RedisCollectionFixture : ICollectionFixture<RedisFixture>
    {
    }
}
