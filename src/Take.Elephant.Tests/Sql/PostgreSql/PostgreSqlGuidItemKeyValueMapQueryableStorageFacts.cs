using Xunit;

namespace Take.Elephant.Tests.Sql.PostgreSql
{
    [Collection(nameof(PostgreSql)), Trait("Category", nameof(PostgreSql))]
    public class PostgreSqlGuidItemKeyValueMapQueryableStorageFacts : SqlGuidItemKeyValueMapQueryableStorageFacts
    {
        public PostgreSqlGuidItemKeyValueMapQueryableStorageFacts(PostgreSqlFixture fixture) : base(fixture)
        {
        }
    }
}
