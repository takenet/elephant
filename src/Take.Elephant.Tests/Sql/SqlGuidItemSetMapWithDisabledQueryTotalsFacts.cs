using System;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using AutoFixture;
using Take.Elephant.Memory;
using Take.Elephant.Sql;
using Take.Elephant.Sql.Mapping;
using Xunit;

namespace Take.Elephant.Tests.Sql
{
    public abstract class SqlGuidItemSetMapWithDisabledQueryTotalsFacts : SqlGuidItemSetMapFacts
    {

        protected SqlGuidItemSetMapWithDisabledQueryTotalsFacts(ISqlFixture serverFixture)
            : base(serverFixture)
        {
        }

        public override IMap<Guid, ISet<Item>> Create()
        {
            var sqlSetMap = base.Create() as SqlSetMap<Guid, Item>;
            sqlSetMap.FetchQueryResultTotal = false;
            return sqlSetMap;
        }

        [Fact(DisplayName = "QueryFromReturnedSetSucceeds")]
        public override async Task QueryFromReturnedSetSucceeds()
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
            AssertEquals(queryResult.Total, 0); // When FetchQueryResultTotal, Total will always be 0;
            AssertIsTrue(await queryResult.Items.ContainsAsync(item1));
            AssertIsTrue(await queryResult.Items.ContainsAsync(item3));
            AssertIsFalse(await queryResult.Items.ContainsAsync(item4));
        }
    }
}
