using Xunit;

namespace Take.Elephant.Tests.Sql.SqlServer
{
    [Collection(nameof(SqlServer)), Trait("Category", nameof(SqlServer))]
    public class SqlServerGuidItemSetMapFacts : SqlGuidItemSetMapFacts
    {
        public SqlServerGuidItemSetMapFacts(SqlServerFixture serverFixture) : base(serverFixture)
        {
        }
    }
}
