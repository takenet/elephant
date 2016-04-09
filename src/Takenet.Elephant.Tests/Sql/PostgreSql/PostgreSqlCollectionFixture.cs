using Xunit;

namespace Takenet.Elephant.Tests.Sql.PostgreSql
{
    [CollectionDefinition(nameof(PostgreSql))]
    public class PostgreSqlCollectionFixture : ICollectionFixture<PostgreSqlFixture>
    {
    }
}
