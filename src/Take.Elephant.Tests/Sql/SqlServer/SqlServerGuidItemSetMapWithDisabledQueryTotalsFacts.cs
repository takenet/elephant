using Xunit;

namespace Take.Elephant.Tests.Sql.SqlServer
{
    [Collection(nameof(SqlServer)), Trait("Category", nameof(SqlServer))]
    public class SqlServerGuidItemSetMapWithDisabledQueryTotalsFacts : SqlGuidItemSetMapWithDisabledQueryTotalsFacts
    {
        public SqlServerGuidItemSetMapWithDisabledQueryTotalsFacts(SqlServerFixture serverFixture) : base(serverFixture)
        {
        }
    }
}
