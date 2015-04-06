using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Takenet.SimplePersistence.Tests.Sql
{
    [CollectionDefinition("Sql")]
    public class SqlCollectionFixture : ICollectionFixture<SqlConnectionFixture>
    {
    }
}
