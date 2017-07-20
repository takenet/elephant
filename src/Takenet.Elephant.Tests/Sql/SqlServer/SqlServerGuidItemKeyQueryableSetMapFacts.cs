using Xunit;

namespace Takenet.Elephant.Tests.Sql.SqlServer
{
    [Collection(nameof(SqlServer)), Trait("Category", nameof(SqlServer))]
    public class SqlServerGuidItemKeyQueryableSetMapFacts : SqlGuidItemKeyQueryableSetMapFacts
    {
        public SqlServerGuidItemKeyQueryableSetMapFacts(SqlServerFixture serverFixture) : base(serverFixture)
        {
        }
    }
}
