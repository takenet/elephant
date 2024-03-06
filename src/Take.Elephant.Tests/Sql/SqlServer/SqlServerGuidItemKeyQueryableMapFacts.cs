using Xunit;

namespace Take.Elephant.Tests.Sql.SqlServer
{
    [Collection(nameof(SqlServer)), Trait("Category", nameof(SqlServer))]
    public class SqlServerGuidItemKeyQueryableMapFacts : SqlGuidItemKeyQueryableMapFacts
    {
        public SqlServerGuidItemKeyQueryableMapFacts(SqlServerFixture serverFixture) : base(serverFixture)
        {
        }
    }
}
