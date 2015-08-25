using System;
using System.Collections.Generic;
using System.Data;
using Takenet.Elephant.Sql;
using Takenet.Elephant.Sql.Mapping;
using Xunit;

namespace Takenet.Elephant.Tests.Sql
{
    [Collection("Sql")]
    public class SqlGuidSetFacts : GuidSetFacts
    {
        private readonly SqlFixture _fixture;

        public SqlGuidSetFacts(SqlFixture fixture)
        {
            _fixture = fixture;
        }
            
        public override ISet<Guid> Create()
        {
            var databaseDriver = new SqlDatabaseDriver();
            var table = new Table("Guids", new [] {"Value"}, new Dictionary<string, SqlType>() { { "Value", new SqlType(DbType.Guid) }});
            _fixture.DropTable(table.Name);
            var mapper = new ValueMapper<Guid>("Value");
            return new SqlSet<Guid>(databaseDriver, _fixture.ConnectionString, table, mapper);
        }
    }
}
