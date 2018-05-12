using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Take.Elephant.Tests.Sql.SqlServer
{
    [Collection(nameof(SqlServer)), Trait("Category", nameof(SqlServer))]
    public class SqlServerGuidItemMapQueryableStorageFacts : SqlGuidItemMapQueryableStorageFacts
    {
        public SqlServerGuidItemMapQueryableStorageFacts(SqlServerFixture serverFixture) : base(serverFixture)
        {
        }
    }
}
