using Xunit;

namespace Take.Elephant.Tests.Sql.PostgreSql
{
    [Collection(nameof(PostgreSql)), Trait("Category", nameof(PostgreSql))]
    public class PostgreSqlIntegerStringMapFacts : SqlIntegerStringMapFacts
    {
        public PostgreSqlIntegerStringMapFacts(PostgreSqlFixture fixture) : base(fixture)
        {
        }
    }
}
