using System.Collections.Generic;
using System.Data;
using Takenet.SimplePersistence.Sql;
using Takenet.SimplePersistence.Sql.Mapping;
using Xunit;

namespace Takenet.SimplePersistence.Tests.Sql
{
    [Collection("Sql")]
    public class SqlIntegerStringMapFacts : IntegerStringMapFacts
    {
        private readonly SqlConnectionFixture _fixture;

        public SqlIntegerStringMapFacts(SqlConnectionFixture fixture)
        {
            _fixture = fixture;
        }

        public override IMap<int, string> Create()
        {
            var databaseDriver = new SqlDatabaseDriver();
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
            return new SqlMap<int, string>(databaseDriver, _fixture.ConnectionString, table, keyMapper, valueMapper);
        }
    }
}
