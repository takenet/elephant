using System;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Ploeh.AutoFixture;
using Take.Elephant.Memory;
using Take.Elephant.Sql;
using Take.Elephant.Sql.Mapping;
using Xunit;

namespace Take.Elephant.Tests.Sql
{
    public abstract class SqlGuidItemSetMapFacts : GuidItemSetMapFacts
    {
        private readonly ISqlFixture _serverFixture;

        protected SqlGuidItemSetMapFacts(ISqlFixture serverFixture)
        {
            _serverFixture = serverFixture;
        }

        public override IMap<Guid, ISet<Item>> Create()
        {
            var columns = typeof(Item)
                .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .ToSqlColumns();
            columns.Add("Key", new SqlType(DbType.Guid));
            var table = new Table("GuidItems", new[] { "Key", nameof(Item.GuidProperty) }, columns, "any");
            _serverFixture.DropTable(table.Schema, table.Name);
            var keyMapper = new ValueMapper<Guid>("Key");
            var valueMapper = new TypeMapper<Item>(table);
            return new SqlSetMap<Guid, Item>(_serverFixture.DatabaseDriver, _serverFixture.ConnectionString, table, keyMapper, valueMapper);
        }

        public override ISet<Item> CreateValue(Guid key, bool populate)
        {
            var set = new Set<Item>();
            if (populate)
            {
                set.AddAsync(Fixture.Create<Item>()).Wait();
                set.AddAsync(Fixture.Create<Item>()).Wait();
                set.AddAsync(Fixture.Create<Item>()).Wait();
            }
            return set;
        }

        [Fact(DisplayName = "QueryFromReturnedSetSucceeds")]
        public virtual async Task QueryFromReturnedSetSucceeds()
        {
            // Arrange
            var expected = "Expected";

            var key1 = CreateKey();
            var set1 = CreateValue(key1, true);
            var map = Create();
            if (!await map.TryAddAsync(key1, set1)) throw new Exception("The test setup failed");
            var key2 = CreateKey();
            var set2 = CreateValue(key2, false);
            var item1 = Fixture.Create<Item>();
            item1.StringProperty = expected;
            var item2 = Fixture.Create<Item>();
            item2.StringProperty = "Unexpected";
            var item3 = Fixture.Create<Item>();
            item3.StringProperty = expected;
            await set2.AddAsync(item1);
            await set2.AddAsync(item2);
            await set2.AddAsync(item3);
            if (!await map.TryAddAsync(key2, set2)) throw new Exception("The test setup failed");
            var key3 = CreateKey();
            var set3 = CreateValue(key3, false);
            var item4 = Fixture.Create<Item>();
            item4.StringProperty = expected;
            await set3.AddAsync(item4);
            if (!await map.TryAddAsync(key3, set3)) throw new Exception("The test setup failed");

            // Act
            var actualSet2 = (IQueryableStorage<Item>)await map.GetValueOrDefaultAsync(key2);
            var queryResult = await actualSet2.QueryAsync<Item>(i => i.StringProperty == expected, null, 0, 10,
                CancellationToken.None);

            // Assert
            AssertEquals(queryResult.Total, 2);
            AssertIsTrue(await queryResult.Items.ContainsAsync(item1));
            AssertIsTrue(await queryResult.Items.ContainsAsync(item3));
            AssertIsFalse(await queryResult.Items.ContainsAsync(item4));
        }
    }
}
