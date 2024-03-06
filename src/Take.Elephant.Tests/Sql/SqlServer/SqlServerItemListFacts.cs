using Xunit;

namespace Take.Elephant.Tests.Sql.SqlServer
{
    [Collection(nameof(SqlServer)), Trait("Category", nameof(SqlServer))]
    public class SqlServerItemListFacts : SqlItemListFacts
    {
        public SqlServerItemListFacts(SqlServerFixture serverFixture) : base(serverFixture)
        {
        }
    }
}
