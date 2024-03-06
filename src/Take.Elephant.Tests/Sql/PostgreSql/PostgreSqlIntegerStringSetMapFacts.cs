using Xunit;

namespace Take.Elephant.Tests.Sql.PostgreSql
{
    [Collection(nameof(PostgreSql)), Trait("Category", nameof(PostgreSql))]
    public class PostgreSqlIntegerStringSetMapFacts : SqlIntegerStringSetMapFacts
    {
        public PostgreSqlIntegerStringSetMapFacts(PostgreSqlFixture fixture) : base(fixture)
        {
        }
    }
}
