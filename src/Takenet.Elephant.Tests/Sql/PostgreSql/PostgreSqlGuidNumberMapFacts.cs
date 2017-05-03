using Takenet.Elephant.Tests.Sql.PostgreSql;
using Xunit;

namespace Takenet.Elephant.Tests.Sql.SqlServer
{
    [Collection(nameof(PostgreSql))]
    public class PostgreSqlGuidNumberMapFacts : SqlGuidNumberMapFacts
    {
        public PostgreSqlGuidNumberMapFacts(PostgreSqlFixture serverFixture) : base(serverFixture)
        {
        }
    }
}
