using Xunit;

namespace Takenet.Elephant.Tests.Sql.SqlServer
{
    [CollectionDefinition(nameof(SqlServer))]
    public class SqlServerCollectionFixture : ICollectionFixture<SqlServerFixture>
    {
    }
}
