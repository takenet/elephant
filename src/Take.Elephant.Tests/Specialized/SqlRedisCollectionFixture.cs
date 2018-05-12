using Xunit;

namespace Take.Elephant.Tests.Specialized
{
    [CollectionDefinition("SqlRedis")]
    public class SqlRedisCollectionFixture : ICollectionFixture<SqlRedisFixture>
    {
    }
}
