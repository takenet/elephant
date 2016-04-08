using System;
using System.Data;
using System.Reflection;
using Takenet.Elephant.Sql;
using Takenet.Elephant.Sql.Mapping;
using Xunit;

namespace Takenet.Elephant.Tests.Sql
{
    [Collection("Sql")]
    public class SqlGuidItemItemSetMapFacts : GuidItemItemSetMapFacts
    {
        private readonly SqlFixture _fixture;

        public SqlGuidItemItemSetMapFacts(SqlFixture fixture)
        {
            _fixture = fixture;
        }

        public override IItemSetMap<Guid, Item> Create()
        {            
            var columns = typeof(Item)
                .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .ToSqlColumns();
            columns.Add("Key", new SqlType(DbType.Guid));
            var table = new Table("GuidItems", new[] { "Key", nameof(Item.GuidProperty) }, columns);
            _fixture.DropTable(table.Name);
            var keyMapper = new ValueMapper<Guid>("Key");
            var valueMapper = new TypeMapper<Item>(table);
            return new SqlSetMap<Guid, Item>(_fixture.DatabaseDriver, _fixture.ConnectionString, table, keyMapper, valueMapper);
        }
    }
}
