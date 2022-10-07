using System.Threading.Tasks;
using Take.Elephant.Sql;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Tests.Sql
{
    public abstract class SqlItemDistinctQueryableStorageFacts : ItemDistinctQueryableStorageFacts
    {
        private readonly ISqlFixture _serverFixture;

        protected SqlItemDistinctQueryableStorageFacts(ISqlFixture serverFixture)
        {
            _serverFixture = serverFixture;
        }

        public override async Task<IDistinctQueryableStorage<Item>> CreateAsync(params Item[] values)
        {
            var table = TableBuilder.WithName("OrderedItemsSet")
                .WithColumnsFromTypeProperties<Item>()
                .WithSynchronizationStrategy(SchemaSynchronizationStrategy.UntilSuccess)
                .Build();
            _serverFixture.DropTable(table.Schema, table.Name);
            var mapper = new TypeMapper<Item>(table);
            var list = new SqlList<Item>(_serverFixture.DatabaseDriver, _serverFixture.ConnectionString, table, mapper);
            foreach (var value in values)
            {
                await list.AddAsync(value);
            }
            return list;
        }
    }
}