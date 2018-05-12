using Xunit;

namespace Take.Elephant.Tests.Sql.SqlServer
{
    [CollectionDefinition(nameof(SqlServer))]
    public class SqlServerCollectionFixture : ICollectionFixture<SqlServerFixture>
    {
    }
}
