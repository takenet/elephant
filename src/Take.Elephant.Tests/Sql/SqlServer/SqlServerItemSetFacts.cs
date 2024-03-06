using Xunit;

namespace Take.Elephant.Tests.Sql.SqlServer
{
    [Collection(nameof(SqlServer)), Trait("Category", nameof(SqlServer))]
    public class SqlServerItemSetFacts : SqlItemSetFacts
    {
        public SqlServerItemSetFacts(SqlServerFixture serverFixture) : base(serverFixture)
        {
        }
    }
}
