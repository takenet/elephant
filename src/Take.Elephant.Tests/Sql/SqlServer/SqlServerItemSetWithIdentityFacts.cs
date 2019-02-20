using Xunit;

namespace Take.Elephant.Tests.Sql.SqlServer
{
    [Collection(nameof(SqlServer)), Trait("Category", nameof(SqlServer))]
    public class SqlServerItemSetWithIdentityFacts : SqlItemSetWithIdentityFacts
    {
        public SqlServerItemSetWithIdentityFacts(SqlServerFixture serverFixture) : base(serverFixture)
        {
        }
    }
}
