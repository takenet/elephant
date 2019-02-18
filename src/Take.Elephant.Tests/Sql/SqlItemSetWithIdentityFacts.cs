using Take.Elephant.Sql;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Tests.Sql
{
    public abstract class SqlItemSetWithIdentityFacts : ItemSetFacts
    {
        private readonly ISqlFixture _serverFixture;

        protected SqlItemSetWithIdentityFacts(ISqlFixture serverFixture)
        {
            _serverFixture = serverFixture;
        }

        public override ISet<Item> Create()
        {
            var table = TableBuilder
                .WithName("ItemsSetWithIdentity")
                .WithKeyColumnFromType<int>(nameof(Item.IntegerProperty), isIdentity: true)
                .WithColumnsFromTypeProperties<Item>(/*p => !p.Name.Equals(nameof(Item.IntegerProperty))*/)
                .Build();
            _serverFixture.DropTable(table.Schema, table.Name);
            var mapper = new TypeMapper<Item>(table);
            return new SqlSet<Item>(_serverFixture.DatabaseDriver, _serverFixture.ConnectionString, table, mapper);
        }
    }
}
