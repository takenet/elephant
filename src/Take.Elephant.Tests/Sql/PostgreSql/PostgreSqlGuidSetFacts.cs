using Xunit;

namespace Take.Elephant.Tests.Sql.PostgreSql
{
    [Collection(nameof(PostgreSql)), Trait("Category", nameof(PostgreSql))]
    public class PostgreSqlGuidSetFacts : SqlGuidSetFacts
    {
        public PostgreSqlGuidSetFacts(PostgreSqlFixture fixture) : base(fixture)
        {
        }
    }
}
