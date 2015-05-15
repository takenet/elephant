using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Takenet.SimplePersistence.Sql;
using Takenet.SimplePersistence.Sql.Mapping;
using Xunit;

namespace Takenet.SimplePersistence.Tests.Sql
{
    [Collection("Sql")]
    public class SqlItemSetFacts : ItemSetFacts
    {
        private readonly SqlConnectionFixture _fixture;

        public SqlItemSetFacts(SqlConnectionFixture fixture)
        {
            _fixture = fixture;
        }

        public override ISet<Item> Create()
        {
            var databaseDriver = new SqlDatabaseDriver();
            var table = TableBuilder.WithName("Items").WithKeyColumnsFromTypeProperties<Item>().Build();
            _fixture.DropTable(table.Name);
            var mapper = new TypeMapper<Item>(table);
            return new SqlSet<Item>(databaseDriver, _fixture.ConnectionString, table, mapper);
        }
    }
}
