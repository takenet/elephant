using System.Collections.Generic;
using System.Data;
using Takenet.Elephant.Sql;
using Takenet.Elephant.Sql.Mapping;

namespace Takenet.Elephant.Tests.Sql
{
    public abstract class SqlIntegerStringMapFacts : IntegerStringMapFacts
    {
        private readonly ISqlFixture _serverFixture;

        protected SqlIntegerStringMapFacts(ISqlFixture serverFixture)
        {
            _serverFixture = serverFixture;
        }

        public override IMap<int, string> Create()
        {
            var table = new Table(
                "IntegerStrings", 
                new[] { "Key"}, 
                new Dictionary<string, SqlType>
                {
                    {"Key", new SqlType(DbType.Int32)},
                    {"Value", new SqlType(DbType.String)}
                });        
            _serverFixture.DropTable(table.Schema, table.Name);
            var keyMapper = new ValueMapper<int>("Key");
            var valueMapper = new ValueMapper<string>("Value");
            return new SqlMap<int, string>(_serverFixture.DatabaseDriver, _serverFixture.ConnectionString, table, keyMapper, valueMapper);
        }
    }
}
