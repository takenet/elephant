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
            var table = TableBuilder.WithName("ItemsSet").WithKeyColumnsFromTypeProperties<Item>().Build();
            _fixture.DropTable(table.Name);
            var mapper = new TypeMapper<Item>(table);
            return new SqlSet<Item>(_fixture.DatabaseDriver, _fixture.ConnectionString, table, mapper);
        }
    }
}
