using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Take.Elephant.Tests.Sql.SqlServer
{
    [Collection(nameof(SqlServer)), Trait("Category", nameof(SqlServer))]
    public class SqlServerGuidSetFacts : SqlGuidSetFacts
    {
        public SqlServerGuidSetFacts(SqlServerFixture serverFixture) : base(serverFixture)
        {
        }
    }
}
