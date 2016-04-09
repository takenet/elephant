using Xunit;

namespace Takenet.Elephant.Tests.Sql.PostgreSql
{
    [Collection(nameof(PostgreSql))]
    public class PostgreSqlGuidItemKeyValueMapQueryableStorageFacts : SqlGuidItemKeyValueMapQueryableStorageFacts
    {
        public PostgreSqlGuidItemKeyValueMapQueryableStorageFacts(PostgreSqlFixture fixture) : base(fixture)
        {
        }
    }
}
