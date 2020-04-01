using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Sql;
using Take.Elephant.Sql.Mapping;
using Xunit;

namespace Take.Elephant.Tests.Sql
{    
    public abstract class SqlGuidItemKeyQueryableSetMapFacts : GuidItemKeyQueryableMapFacts
    {
        private readonly ISqlFixture _serverFixture;

        protected SqlGuidItemKeyQueryableSetMapFacts(ISqlFixture serverFixture)
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
            var map = new SqlSetMap<Guid, Item>(_serverFixture.DatabaseDriver, _serverFixture.ConnectionString, table, keyMapper, valueMapper);

            foreach (var value in values)
            {
                await map.AddItemAsync(value.Key, value.Value);
            }

            return map;
        }

        [Fact(DisplayName = "QueryEmptyContainsExpressionReturnsNone")]
        public async Task QueryEmptyContainsExpressionReturnsNone()
        {
            // Arrange
            var key1 = CreateKey();
            var key2 = CreateKey();
            var value1 = CreateValue(key1);
            var value2 = CreateValue(key2);
            var map = await CreateAsync(
                new KeyValuePair<Guid,Item>(key1, value1),
                new KeyValuePair<Guid,Item>(key2, value2));
            var skip = 0;
            var take = 5;

            var filter = new Guid[] {  }.GetContainsExpressionForGuidProperty();

            // Act
            var actual = await map.QueryForKeysAsync<Guid>(filter, null, skip, take, CancellationToken.None);

            // Assert
            AssertEquals(actual.Total, 0);
            var actualList = await actual.ToListAsync();
            AssertEquals(actualList.Count, 0);
        }
    }
}
