﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Take.Elephant.Tests.Sql.SqlServer
{
    [Collection(nameof(SqlServer)), Trait("Category", nameof(SqlServer))]
    public class SqlServerIntegerStringSetMapFacts : SqlIntegerStringSetMapFacts
    {
        public SqlServerIntegerStringSetMapFacts(SqlServerFixture serverFixture) : base(serverFixture)
        {
        }
    }
}
