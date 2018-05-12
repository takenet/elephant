using Xunit;

namespace Take.Elephant.Tests.Sql.SqlServer
{
    [Collection(nameof(SqlServer)), Trait("Category", nameof(SqlServer))]
    public class SqlServerGuidItemKeyValueMapQueryableStorageFacts : SqlGuidItemKeyValueMapQueryableStorageFacts
    {
        public SqlServerGuidItemKeyValueMapQueryableStorageFacts(SqlServerFixture serverFixture) : base(serverFixture)
        {
        }
    }
}
