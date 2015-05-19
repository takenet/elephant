using Xunit;

namespace Takenet.Elephant.Tests.Redis
{
    [CollectionDefinition("Redis")]
    public class RedisCollectionFixture : ICollectionFixture<RedisFixture>
    {
    }
}
