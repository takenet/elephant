using Takenet.Elephant.Sql;
using Takenet.Elephant.Sql.Mapping;
using Xunit;

namespace Takenet.Elephant.Tests.Sql
{
    [Collection("Sql")]
    public class SqlItemSetFacts : ItemSetFacts
    {
        private readonly SqlFixture _fixture;

        public SqlItemSetFacts(SqlFixture fixture)
        {
            _fixture = fixture;
        }

        public override ISet<Item> Create()
        {
            var databaseDriver = new SqlDatabaseDriver();
            var table = TableBuilder.WithName("Items").WithKeyColumnsFromTypeProperties<Item>().Build();
            _fixture.DropTable(table.Name);
            var mapper = new TypeMapper<Item>(table);
            return new SqlSet<Item>(databaseDriver, _fixture.ConnectionString, table, mapper);
        }
    }
}
