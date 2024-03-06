using Xunit;

namespace Take.Elephant.Tests.Sql.PostgreSql
{
    [Collection(nameof(PostgreSql)), Trait("Category", nameof(PostgreSql))]
    public class PostgreSqlGuidItemKeyQueryableSetMapFacts : SqlGuidItemKeyQueryableSetMapFacts
    {
        public PostgreSqlGuidItemKeyQueryableSetMapFacts(PostgreSqlFixture fixture) : base(fixture)
        {
        }
    }
}
