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
    public class SqlItemSetFacts : ItemSetFacts, IClassFixture<SqlConnectionFixture>
    {
        private readonly SqlConnectionFixture _fixture;

        public SqlItemSetFacts(SqlConnectionFixture fixture)
        {
            _fixture = fixture;
        }

        public override ISet<Item> Create()
        {
            var table = new TypeTable<Item>("Items", typeof(Item).GetProperties().Select(p => p.Name).ToArray());
            _fixture.DropTable(table.Name);
            return new SqlItemSet(table, _fixture.ConnectionString);
        }

        private class SqlItemSet : SqlSet<Item>
        {
            public SqlItemSet(ITable table, string connectionString) : base(table, connectionString)
            {
            }

            protected override IMapper<Item> Mapper => new TypeMapper<Item>(Table);
        }
    }
}
