using Xunit;

namespace Takenet.Elephant.Tests.Sql.PostgreSql
{
    [Collection(nameof(PostgreSql))]
    public class PostgreSqlGuidItemItemSetMapFacts : SqlGuidItemItemSetMapFacts
    {        
        public PostgreSqlGuidItemItemSetMapFacts(PostgreSqlFixture fixture)
            : base(fixture)
        {
        }
    }
}