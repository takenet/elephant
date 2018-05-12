using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Take.Elephant.Tests.Sql.SqlServer;
using Xunit;

namespace Take.Elephant.Tests.Sql.PostgreSql
{
    [Collection(nameof(PostgreSql)), Trait("Category", nameof(PostgreSql))]
    public class PostgreSqlItemDistinctQueryableStorageFacts : SqlItemDistinctQueryableStorageFacts
    {
        public PostgreSqlItemDistinctQueryableStorageFacts(PostgreSqlFixture fixture) : base(fixture)
        {
        }
    }
}
