using Xunit;

namespace Take.Elephant.Tests.Sql.PostgreSql
{
    [Collection(nameof(PostgreSql)), Trait("Category", nameof(PostgreSql))]
    public class PostgreSqlGuidItemPropertyMapFacts : SqlGuidItemPropertyMapFacts
    {
        public PostgreSqlGuidItemPropertyMapFacts(PostgreSqlFixture fixture) : base(fixture)
        {
        }
    }
}
