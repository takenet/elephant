using Xunit;

namespace Takenet.Elephant.Tests.Sql.SqlServer
{
    [Collection(nameof(SqlServer))]
    public class SqlServerGuidItemKeyQueryableSetMapFacts : SqlGuidItemKeyQueryableSetMapFacts
    {
        public SqlServerGuidItemKeyQueryableSetMapFacts(SqlServerFixture serverFixture) : base(serverFixture)
        {
        }
    }
}
