using Xunit;

namespace Take.Elephant.Tests.Sql.PostgreSql
{
    [Collection(nameof(PostgreSql)), Trait("Category", nameof(PostgreSql))]
    public class PostgreSqlItemSetFacts : SqlItemSetFacts
    {
        public PostgreSqlItemSetFacts(PostgreSqlFixture fixture) : base(fixture)
        {
        }
    }
}
