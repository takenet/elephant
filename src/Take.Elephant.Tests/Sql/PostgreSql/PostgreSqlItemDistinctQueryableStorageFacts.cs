﻿using Xunit;

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