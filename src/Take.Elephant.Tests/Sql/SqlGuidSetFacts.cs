using System;
using System.Collections.Generic;
using System.Data;
using Take.Elephant.Sql;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Tests.Sql
{
    public abstract class SqlGuidSetFacts : GuidItemSetFacts
    {
        private readonly ISqlFixture _serverFixture;

        protected SqlGuidSetFacts(ISqlFixture serverFixture)
        {
            _serverFixture = serverFixture;
        }
            
        public override ISet<Guid> Create()
        {
            var table = new Table("Guids", new [] {"Value"}, new Dictionary<string, SqlType>() { { "Value", new SqlType(DbType.Guid) }}, "test", synchronizationStrategy: SchemaSynchronizationStrategy.UntilSuccess);
            _serverFixture.DropTable(table.Schema, table.Name);
            var mapper = new ValueMapper<Guid>("Value");
            return new SqlSet<Guid>(_serverFixture.DatabaseDriver, _serverFixture.ConnectionString, table, mapper);
        }
    }
}
