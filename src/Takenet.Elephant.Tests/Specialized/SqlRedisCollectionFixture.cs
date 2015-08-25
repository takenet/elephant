using Xunit;

namespace Takenet.Elephant.Tests.Specialized
{
    [CollectionDefinition("SqlRedis")]
    public class SqlRedisCollectionFixture : ICollectionFixture<SqlRedisFixture>
    {
    }
}
