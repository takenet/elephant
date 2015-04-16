using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Takenet.SimplePersistence.Sql.Mapping;
using Xunit;

namespace Takenet.SimplePersistence.Tests.Sql
{
    [Collection("Sql")]
    public class SqlGuidItemMapQueryableStorageFacts : GuidItemMapQueryableStorageFacts
    {
        private readonly SqlConnectionFixture _fixture;

        public SqlGuidItemMapQueryableStorageFacts(SqlConnectionFixture fixture)
        {
            _fixture = fixture;
        }

        public override IMap<Guid, Item> Create()
        {
            var columns = typeof(Item)
                .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .ToSqlColumns();
            columns.Add("Key", new SqlType(DbType.Guid));
            var table = new Table("GuidItems", new[] { "Key" }, columns);
            _fixture.DropTable(table.Name);

            return new GuidItemSqlMap(table, _fixture.ConnectionString);
        }
    }
}
