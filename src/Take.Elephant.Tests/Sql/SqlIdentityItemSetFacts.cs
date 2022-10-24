using System.Threading.Tasks;
using Take.Elephant.Sql;
using Take.Elephant.Sql.Mapping;
using Xunit;

namespace Take.Elephant.Tests.Sql
{
    public abstract class SqlIdentityItemSetFacts : ItemSetFacts
    {
        private readonly ISqlFixture _serverFixture;

        protected SqlIdentityItemSetFacts(ISqlFixture serverFixture)
        {
            _serverFixture = serverFixture;
        }

        public override Item CreateItem()
        {
            var item = base.CreateItem();
            item.IntegerProperty = 0;
            return item;
        }

        public override ISet<Item> Create()
        {
            var table = TableBuilder
                .WithName("IdentityItemsSet")
                .WithColumnsFromTypeProperties<Item>()
                .WithKeyColumnFromType<int>(nameof(Item.IntegerProperty), true)
                .WithSynchronizationStrategy(SchemaSynchronizationStrategy.UntilSuccess)
                .Build();

            _serverFixture.DropTable(table.Schema, table.Name);
            var mapper = new TypeMapper<Item>(table);
            return new SqlSet<Item>(_serverFixture.DatabaseDriver, _serverFixture.ConnectionString, table, mapper);
        }        
    }
}
