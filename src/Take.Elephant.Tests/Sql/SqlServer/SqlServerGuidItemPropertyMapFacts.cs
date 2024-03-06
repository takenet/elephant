﻿using Xunit;

namespace Take.Elephant.Tests.Sql.SqlServer
{
    [Collection(nameof(SqlServer)), Trait("Category", nameof(SqlServer))]
    public class SqlServerGuidItemPropertyMapFacts : SqlGuidItemPropertyMapFacts
    {
        public SqlServerGuidItemPropertyMapFacts(SqlServerFixture serverFixture) : base(serverFixture)
        {
        }
    }
}
