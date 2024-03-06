using Xunit;

namespace Take.Elephant.Tests.Sql.PostgreSql
{
    [Collection(nameof(PostgreSql)), Trait("Category", nameof(PostgreSql))]
    public class PostgreSqlItemListFacts : SqlItemListFacts
    {
        public PostgreSqlItemListFacts(PostgreSqlFixture fixture) : base(fixture)
        {
        }
    }
}
