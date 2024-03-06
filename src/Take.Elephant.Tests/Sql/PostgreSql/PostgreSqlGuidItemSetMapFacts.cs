using Xunit;

namespace Take.Elephant.Tests.Sql.PostgreSql
{
    [Collection(nameof(PostgreSql)), Trait("Category", nameof(PostgreSql))]
    public class PostgreSqlGuidItemSetMapFacts : SqlGuidItemSetMapFacts
    {
        public PostgreSqlGuidItemSetMapFacts(PostgreSqlFixture fixture) : base(fixture)
        {
        }
    }
}
