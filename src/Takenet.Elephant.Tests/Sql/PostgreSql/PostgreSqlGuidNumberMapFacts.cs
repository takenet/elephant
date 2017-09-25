using Xunit;

namespace Takenet.Elephant.Tests.Sql.PostgreSql
{
    [Collection(nameof(PostgreSql)), Trait("Category", nameof(PostgreSql))]
    public class PostgreSqlGuidNumberMapFacts : SqlGuidNumberMapFacts
    {
        public PostgreSqlGuidNumberMapFacts(PostgreSqlFixture serverFixture) : base(serverFixture)
        {
        }
    }
}
