using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using System.Threading.Tasks;
using Takenet.Elephant.Sql;
using Takenet.Elephant.Sql.Mapping;
using Xunit;

namespace Takenet.Elephant.Tests.Sql
{
    [Collection("Sql")]
    public class SqlGuidItemKeyQueryableMapFacts : GuidItemKeyQueryableMapFacts
    {
        private readonly SqlFixture _fixture;

        public SqlGuidItemKeyQueryableMapFacts(SqlFixture fixture)
        {
            _fixture = fixture;
        }

        public async override Task<IKeyQueryableMap<Guid, Item>> CreateAsync(params KeyValuePair<Guid, Item>[] values)
        {
            var databaseDriver = new SqlDatabaseDriver();
            var columns = typeof(Item)
                .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .ToSqlColumns();
            columns.Add("Key", new SqlType(DbType.Guid));
            var table = new Table("GuidItems", new[] { "Key" }, columns);
            _fixture.DropTable(table.Name);

            var keyMapper = new ValueMapper<Guid>("Key");
            var valueMapper = new TypeMapper<Item>(table);
            var map = new SqlMap<Guid, Item>(databaseDriver, _fixture.ConnectionString, table, keyMapper, valueMapper);

            foreach (var value in values)
            {
                await map.TryAddAsync(value.Key, value.Value);
            }

            return map;
        }
    }
}
