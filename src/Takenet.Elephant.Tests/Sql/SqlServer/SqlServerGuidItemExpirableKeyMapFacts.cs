using Xunit;

namespace Takenet.Elephant.Tests.Sql.SqlServer
{
    [Collection(nameof(SqlServer)), Trait("Category", nameof(SqlServer))]
    public class SqlServerGuidItemExpirableKeyMapFacts : SqlGuidItemExpirableKeyMapFacts
    {
        public SqlServerGuidItemExpirableKeyMapFacts(SqlServerFixture serverFixture) : base(serverFixture)
        {
        }
    }
}
