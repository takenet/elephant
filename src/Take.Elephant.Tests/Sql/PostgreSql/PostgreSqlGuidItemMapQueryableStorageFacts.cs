using Xunit;

namespace Take.Elephant.Tests.Sql.PostgreSql
{
    [Collection(nameof(PostgreSql)), Trait("Category", nameof(PostgreSql))]
    public class PostgreSqlGuidItemMapQueryableStorageFacts : SqlGuidItemMapQueryableStorageFacts
    {
        public PostgreSqlGuidItemMapQueryableStorageFacts(PostgreSqlFixture fixture) : base(fixture)
        {
        }
    }
}
