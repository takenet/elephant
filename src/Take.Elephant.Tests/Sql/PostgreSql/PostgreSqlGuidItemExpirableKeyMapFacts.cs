using Xunit;

namespace Take.Elephant.Tests.Sql.PostgreSql
{
    [Collection(nameof(PostgreSql)), Trait("Category", nameof(PostgreSql))]
    public class PostgreSqlGuidItemExpirableKeyMapFacts : SqlGuidItemExpirableKeyMapFacts
    {
        public PostgreSqlGuidItemExpirableKeyMapFacts(PostgreSqlFixture serverFixture) : base(serverFixture)
        {
        }
    }
}
