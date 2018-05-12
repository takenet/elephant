using Xunit;

namespace Take.Elephant.Tests.Sql.SqlServer
{
    [Collection(nameof(SqlServer)), Trait("Category", nameof(SqlServer))]
    public class SqlServerItemDistinctQueryableStorageFacts : SqlItemDistinctQueryableStorageFacts
    {
        public SqlServerItemDistinctQueryableStorageFacts(SqlServerFixture serverFixture) : base(serverFixture)
        {
        }
    }
}
