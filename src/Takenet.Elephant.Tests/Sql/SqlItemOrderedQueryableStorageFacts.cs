using System.Threading.Tasks;
using Takenet.Elephant.Sql;
using Takenet.Elephant.Sql.Mapping;

namespace Takenet.Elephant.Tests.Sql
{
    public abstract class SqlItemOrderedQueryableStorageFacts : ItemOrderedQueryableStorageFacts
    {
        private readonly ISqlFixture _serverFixture;

        protected SqlItemOrderedQueryableStorageFacts(ISqlFixture serverFixture)
        {
            _serverFixture = serverFixture;
        }

        public override async Task<IOrderedQueryableStorage<Item>> CreateAsync(params Item[] values)
        {
            var table = TableBuilder.WithName("OrderedItemsSet").WithKeyColumnsFromTypeProperties<Item>().Build();
            _serverFixture.DropTable(table.Schema, table.Name);
            var mapper = new TypeMapper<Item>(table);
            var set = new SqlSet<Item>(_serverFixture.DatabaseDriver, _serverFixture.ConnectionString, table, mapper);
            foreach (var value in values)
            {
                await set.AddAsync(value);
            }
            return set;
        }
    }
}
