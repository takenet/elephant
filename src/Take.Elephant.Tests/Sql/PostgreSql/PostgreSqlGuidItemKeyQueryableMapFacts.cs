using Xunit;

namespace Take.Elephant.Tests.Sql.PostgreSql
{
    [Collection(nameof(PostgreSql)), Trait("Category", nameof(PostgreSql))]
    public class PostgreSqlGuidItemKeyQueryableMapFacts : SqlGuidItemKeyQueryableMapFacts
    {
        public PostgreSqlGuidItemKeyQueryableMapFacts(PostgreSqlFixture fixture) 
            : base(fixture)
        {
        }
    }
}
