using Xunit;

namespace Takenet.Elephant.Tests.Sql.SqlServer
{
    [Collection(nameof(SqlServer)), Trait("Category", nameof(SqlServer))]
    public class SqlServerGuidNumberMapFacts : SqlGuidNumberMapFacts
    {
        public SqlServerGuidNumberMapFacts(SqlServerFixture serverFixture) : base(serverFixture)
        {
        }
    }
}
