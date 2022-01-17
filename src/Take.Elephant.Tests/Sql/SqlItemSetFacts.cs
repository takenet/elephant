using Take.Elephant.Sql;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Tests.Sql
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
            _serverFixture.DropTable(table.Schema, table.Name);
            var mapper = new TypeMapper<Item>(table);
            return Create(table, mapper);
        }

        protected virtual ISet<Item> Create(ITable table, TypeMapper<Item> mapper) => new SqlSet<Item>(_serverFixture.DatabaseDriver, _serverFixture.ConnectionString, table, mapper);
    }
}
