using System.Threading.Tasks;
using Takenet.Elephant.Sql;
using Takenet.Elephant.Sql.Mapping;

namespace Takenet.Elephant.Tests.Sql
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
            var table = TableBuilder.WithName("OrderedItemsSet").WithColumnsFromTypeProperties<Item>().Build();
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