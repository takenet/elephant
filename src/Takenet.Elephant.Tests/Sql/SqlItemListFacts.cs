using Takenet.Elephant.Sql;
using Takenet.Elephant.Sql.Mapping;

namespace Takenet.Elephant.Tests.Sql
{
    public abstract class SqlItemListFacts : ItemListFacts
    {
        private readonly ISqlFixture _serverFixture;

        protected SqlItemListFacts(ISqlFixture serverFixture)
        {
            _serverFixture = serverFixture;
        }

        public override IList<Item> Create()
        {
            var table = TableBuilder.WithName("ItemsSet").WithKeyColumnsFromTypeProperties<Item>().WithKeyColumnFromType<int>("Id", true).Build();
            _serverFixture.DropTable(table.Schema, table.Name);
            var mapper = new TypeMapper<Item>(table);
            return new SqlList<Item>(_serverFixture.DatabaseDriver, _serverFixture.ConnectionString, table, mapper);
        }
    }
}
