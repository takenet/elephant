using Xunit;

namespace Take.Elephant.Tests.Sql.PostgreSql
{
    [CollectionDefinition(nameof(PostgreSql))]
    public class PostgreSqlCollectionFixture : ICollectionFixture<PostgreSqlFixture>
    {
    }
}
