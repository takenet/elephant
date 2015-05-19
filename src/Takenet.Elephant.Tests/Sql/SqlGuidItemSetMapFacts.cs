using System;
using System.Data;
using System.Reflection;
using Ploeh.AutoFixture;
using Takenet.Elephant.Memory;
using Takenet.Elephant.Sql;
using Takenet.Elephant.Sql.Mapping;
using Xunit;

namespace Takenet.Elephant.Tests.Sql
{
    [Collection("Sql")]
    public class SqlGuidItemSetMapFacts : GuidItemSetMapFacts
    {
        private readonly SqlConnectionFixture _fixture;

        public SqlGuidItemSetMapFacts(SqlConnectionFixture fixture)
        {
            _fixture = fixture;
        }

        public override IMap<Guid, ISet<Item>> Create()
        {
            var databaseDriver = new SqlDatabaseDriver();
            var columns = typeof(Item)
                .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .ToSqlColumns();
            columns.Add("Key", new SqlType(DbType.Guid));
            var table = new Table("GuidItems", new[] { "Key", nameof(Item.GuidProperty) }, columns);
            _fixture.DropTable(table.Name);
            var keyMapper = new ValueMapper<Guid>("Key");
            var valueMapper = new TypeMapper<Item>(table);
            return new SqlSetMap<Guid, Item>(databaseDriver, _fixture.ConnectionString, table, keyMapper, valueMapper);
        }

        public override ISet<Item> CreateValue(Guid key)
        {
            var set = new Set<Item>();
            set.AddAsync(Fixture.Create<Item>()).Wait();
            set.AddAsync(Fixture.Create<Item>()).Wait();
            set.AddAsync(Fixture.Create<Item>()).Wait();
            return set;
        }
    }
}
