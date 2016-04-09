using Takenet.Elephant.Sql;
using Takenet.Elephant.Sql.Mapping;

namespace Takenet.Elephant.Tests.Sql
{
    public abstract class SqlItemSetFacts : ItemSetFacts
    {
        private readonly ISqlFixture _serverFixture;

        protected SqlItemSetFacts(ISqlFixture serverFixture)
        {
            _serverFixture = serverFixture;
        }

        public override ISet<Item> Create()
        {
            var table = TableBuilder.WithName("ItemsSet").WithKeyColumnsFromTypeProperties<Item>().Build();
            _serverFixture.DropTable(table.Name);
            var mapper = new TypeMapper<Item>(table);
            return new SqlSet<Item>(_serverFixture.DatabaseDriver, _serverFixture.ConnectionString, table, mapper);
        }
    }
}
