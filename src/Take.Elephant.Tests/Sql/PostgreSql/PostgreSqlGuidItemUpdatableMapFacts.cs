using Xunit;

namespace Take.Elephant.Tests.Sql.PostgreSql
{
    [Collection(nameof(PostgreSql)), Trait("Category", nameof(PostgreSql))]
    public class PostgreSqlGuidItemUpdatableMapFacts : SqlGuidItemUpdatableMapFacts
    {
        public PostgreSqlGuidItemUpdatableMapFacts(PostgreSqlFixture fixture) : base(fixture)
        {
        }
    }
}
