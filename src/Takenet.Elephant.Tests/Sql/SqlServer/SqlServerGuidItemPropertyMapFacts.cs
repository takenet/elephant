using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Takenet.Elephant.Tests.Sql.SqlServer
{
    [Collection(nameof(SqlServer))]
    public class SqlServerGuidItemPropertyMapFacts : SqlGuidItemPropertyMapFacts
    {
        public SqlServerGuidItemPropertyMapFacts(SqlServerFixture serverFixture) : base(serverFixture)
        {
        }
    }
}
