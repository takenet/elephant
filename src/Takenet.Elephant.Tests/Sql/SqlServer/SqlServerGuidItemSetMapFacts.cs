using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Takenet.Elephant.Tests.Sql.SqlServer
{
    [Collection(nameof(SqlServer)), Trait("Category", nameof(SqlServer))]
    public class SqlServerGuidItemSetMapFacts : SqlGuidItemSetMapFacts
    {
        public SqlServerGuidItemSetMapFacts(SqlServerFixture serverFixture) : base(serverFixture)
        {
        }
    }
}
