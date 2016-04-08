using System.Collections.Generic;
using System.Data;
using Takenet.Elephant.Sql;
using Takenet.Elephant.Sql.Mapping;
using Xunit;

namespace Takenet.Elephant.Tests.Sql
{
    [Collection("Sql")]
    public class SqlIntegerStringMapFacts : IntegerStringMapFacts
    {
        private readonly SqlFixture _fixture;

        public SqlIntegerStringMapFacts(SqlFixture fixture)
        {
            _fixture = fixture;
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
            _fixture.DropTable(table.Name);
            var keyMapper = new ValueMapper<int>("Key");
            var valueMapper = new ValueMapper<string>("Value");
            return new SqlMap<int, string>(_fixture.DatabaseDriver, _fixture.ConnectionString, table, keyMapper, valueMapper);
        }
    }
}
