using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Takenet.SimplePersistence.Sql;
using Takenet.SimplePersistence.Sql.Mapping;
using Xunit;

namespace Takenet.SimplePersistence.Tests.Sql
{
    [Collection("Sql")]
    public class SqlGuidItemMapFacts : GuidItemMapFacts
    {
        private readonly SqlConnectionFixture _fixture;

        public SqlGuidItemMapFacts(SqlConnectionFixture fixture)
        {
            _fixture = fixture;
        }

        public override IMap<Guid, Item> Create()
        {
            var columns = typeof (Item).GetProperties(BindingFlags.Instance | BindingFlags.Public).ToSqlColumns();
            columns.Add("Key", new SqlType(DbType.Guid));
            var table = new Table("GuidItems", new[] { "Key" }, columns);
            _fixture.DropTable(table.Name);

            return new GuidItemSqlMap(table, _fixture.ConnectionString);
        }

        private class GuidItemSqlMap : SqlMap<Guid, Item>
        {
            public GuidItemSqlMap(ITable table, string connectionString) : base(table, connectionString)
            {
            }

            protected override IMapper<Item> Mapper => new TypeMapper<Item>(Table);

            protected override IMapper<Guid> KeyMapper => new ValueMapper<Guid>("Key");
        }
        
               
    }
}
