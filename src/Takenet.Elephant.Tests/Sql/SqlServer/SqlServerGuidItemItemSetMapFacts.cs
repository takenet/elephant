using Takenet.Elephant.Tests.Sql.SqlServer;
using Xunit;

namespace Takenet.Elephant.Tests.Sql.SqlServer
{
    [Collection(nameof(SqlServer)), Trait("Category", nameof(SqlServer))]
    public class SqlServerGuidItemItemSetMapFacts : SqlGuidItemItemSetMapFacts
    {        
        public SqlServerGuidItemItemSetMapFacts(SqlServerFixture serverFixture)
            : base(serverFixture)
        {
        }
    }
}