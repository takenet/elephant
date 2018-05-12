using Xunit;

namespace Take.Elephant.Tests.Sql.SqlServer
{
    [Collection(nameof(SqlServer)), Trait("Category", nameof(SqlServer))]
    public class SqlServerGuidItemMapFacts : SqlGuidItemMapFacts
    {
        public SqlServerGuidItemMapFacts(SqlServerFixture serverFixture) : base(serverFixture)
        {
        }
    }
}
