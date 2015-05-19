using System.Collections.Generic;
using System.Data;
using Ploeh.AutoFixture;
using Takenet.Elephant.Memory;
using Takenet.Elephant.Sql;
using Takenet.Elephant.Sql.Mapping;
using Xunit;

namespace Takenet.Elephant.Tests.Sql
{
    [Collection("Sql")]
    public class SqlIntegerStringSetMapFacts : IntegerStringSetMapFacts
    {
        private readonly SqlConnectionFixture _fixture;

        public SqlIntegerStringSetMapFacts(SqlConnectionFixture fixture)
        {
            _fixture = fixture;
        }

        public override IMap<int, ISet<string>> Create()
        {
            var databaseDriver = new SqlDatabaseDriver();
            var table = new Table(
                "IntegerStrings",
                new[] { "Key", "Value" },
                new Dictionary<string, SqlType>
                {
                    {"Key", new SqlType(DbType.Int32)},
                    {"Value", new SqlType(DbType.String)}
                });
            _fixture.DropTable(table.Name);
            var keyMapper = new ValueMapper<int>("Key");
            var valueMapper = new ValueMapper<string>("Value");
            return new SqlSetMap<int, string>(databaseDriver, _fixture.ConnectionString, table, keyMapper, valueMapper);
        }

        public override ISet<string> CreateValue(int key)
        {
            var set = new Set<string>();
            set.AddAsync(Fixture.Create<string>()).Wait();
            set.AddAsync(Fixture.Create<string>()).Wait();
            set.AddAsync(Fixture.Create<string>()).Wait();
            return set;
        }
    }
}
