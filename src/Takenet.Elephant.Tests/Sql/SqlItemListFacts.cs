using Takenet.Elephant.Sql;
using Takenet.Elephant.Sql.Mapping;
using Xunit;

namespace Takenet.Elephant.Tests.Sql
{
    [Collection("Sql")]
    public class SqlItemListFacts : ItemListFacts
    {
        private readonly SqlFixture _fixture;

        public SqlItemListFacts(SqlFixture fixture)
        {
            _fixture = fixture;
        }

        public override IList<Item> Create()
        {
            var databaseDriver = new SqlDatabaseDriver();
            var table = TableBuilder.WithName("ItemsSet").WithKeyColumnsFromTypeProperties<Item>().WithKeyColumnFromType<int>("Id", true).Build();
            _fixture.DropTable(table.Name);
            var mapper = new TypeMapper<Item>(table);
            return new SqlList<Item>(databaseDriver, _fixture.ConnectionString, table, mapper);
        }
    }
}
