using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Takenet.Elephant.Tests.Sql.PostgreSql
{
    [Collection(nameof(PostgreSql))]
    public class PostgreSqlGuidItemMapQueryableStorageFacts : SqlGuidItemMapQueryableStorageFacts
    {
        public PostgreSqlGuidItemMapQueryableStorageFacts(PostgreSqlFixture fixture) : base(fixture)
        {
        }
    }
}
