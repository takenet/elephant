using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using System.Threading.Tasks;
using Take.Elephant.Sql;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Tests.Sql
{
    public abstract class SqlGuidItemKeyQueryableMapFacts : GuidItemKeyQueryableMapFacts
    {
        private readonly ISqlFixture _serverFixture;

        protected SqlGuidItemKeyQueryableMapFacts(ISqlFixture serverFixture)
        {
            _serverFixture = serverFixture;
        }

        public override async Task<IKeyQueryableMap<Guid, Item>> CreateAsync(params KeyValuePair<Guid, Item>[] values)
        {
            var columns = typeof(Item)
                .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .ToSqlColumns();
            columns.Add("Key", new SqlType(DbType.Guid));
            var table = new Table("GuidItems", new[] { "Key" }, columns);
            _serverFixture.DropTable(table.Schema, table.Name);

            var keyMapper = new ValueMapper<Guid>("Key");
            var valueMapper = new TypeMapper<Item>(table);
            var map = new SqlMap<Guid, Item>(_serverFixture.DatabaseDriver, _serverFixture.ConnectionString, table, keyMapper, valueMapper);

            foreach (var value in values)
            {
                await map.TryAddAsync(value.Key, value.Value);
            }

            return map;
        }
    }
}
