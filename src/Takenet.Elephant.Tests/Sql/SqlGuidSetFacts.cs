using System;
using System.Collections.Generic;
using System.Data;
using Takenet.Elephant.Sql;
using Takenet.Elephant.Sql.Mapping;

namespace Takenet.Elephant.Tests.Sql
{
    public abstract class SqlGuidSetFacts : GuidSetFacts
    {
        private readonly ISqlFixture _serverFixture;

        protected SqlGuidSetFacts(ISqlFixture serverFixture)
        {
            _serverFixture = serverFixture;
        }
            
        public override ISet<Guid> Create()
        {
            var table = new Table("Guids", new [] {"Value"}, new Dictionary<string, SqlType>() { { "Value", new SqlType(DbType.Guid) }});
            _serverFixture.DropTable(table.Name);
            var mapper = new ValueMapper<Guid>("Value");
            return new SqlSet<Guid>(_serverFixture.DatabaseDriver, _serverFixture.ConnectionString, table, mapper);
        }
    }
}
