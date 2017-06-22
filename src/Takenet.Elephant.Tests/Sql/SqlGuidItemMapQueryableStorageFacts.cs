using System;
using System.Data;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Takenet.Elephant.Sql;
using Takenet.Elephant.Sql.Mapping;
using Xunit;

namespace Takenet.Elephant.Tests.Sql
{
    public abstract class SqlGuidItemMapQueryableStorageFacts : GuidItemMapQueryableStorageFacts
    {
        private readonly ISqlFixture _serverFixture;

        protected SqlGuidItemMapQueryableStorageFacts(ISqlFixture serverFixture)
        {
            _serverFixture = serverFixture;
        }

        public override IMap<Guid, Item> Create()
        {
            var columns = typeof(Item)
                .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .ToSqlColumns();
            columns.Add("Key", new SqlType(DbType.Guid));
            var table = new Table("GuidItems", new[] { "Key" }, columns);
            _serverFixture.DropTable(table.Schema, table.Name);

            var keyMapper = new ValueMapper<Guid>("Key");
            var valueMapper = new TypeMapper<Item>(table);
            return new SqlMap<Guid, Item>(_serverFixture.DatabaseDriver, _serverFixture.ConnectionString, table, keyMapper, valueMapper);
        }

        [Fact(DisplayName = "QueryExistingValueFilteringWithTakeLimitSucceeds")]
        public virtual async Task QueryExistingValueFilteringWithTakeLimitSucceeds()
        {
            // Arrange            
            var value1 = CreateValue();
            var value2 = CreateValue();
            var value3 = CreateValue();
            var value4 = CreateValue();
            var value5 = CreateValue();
            var cancellationToken = CancellationToken.None;
            var skip = 0;
            var take = 2;
            var storage = await CreateAsync(value1, value2, value3, value4, value5);
            var filter = new[] { value1.GuidProperty, value2.GuidProperty, value3.GuidProperty }.GetContainsExpressionForGuidProperty();

            // Act
            var actual = await storage.QueryAsync<Item>(filter, null, skip, take, cancellationToken);

            // Assert            
            var actualList = await actual.ToListAsync();
            AssertEquals(actualList.Count, 2);
            AssertEquals(actual.Total, 3);
        }

        [Fact(DisplayName = "QueryExistingValueFilteringWithTakeAndSkipLimitSucceeds")]
        public virtual async Task QueryExistingValueFilteringWithTakeAndSkipLimitSucceeds()
        {
            // Arrange            
            var value1 = CreateValue();
            var value2 = CreateValue();
            var value3 = CreateValue();
            var value4 = CreateValue();
            var value5 = CreateValue();
            var cancellationToken = CancellationToken.None;
            var skip = 2;
            var take = 2;
            var storage = await CreateAsync(value1, value2, value3, value4, value5);
            var filter = new[] { value1.GuidProperty, value2.GuidProperty, value3.GuidProperty }.GetContainsExpressionForGuidProperty();

            // Act
            var actual = await storage.QueryAsync<Item>(filter, null, skip, take, cancellationToken);

            // Assert            
            var actualList = await actual.ToListAsync();
            AssertEquals(actualList.Count, 1);
            AssertEquals(actual.Total, 3);
        }

        [Fact(DisplayName = "QueryExistingValueFilteringWithSkipLimitAfterLengthReturnsEmpty")]
        public virtual async Task QueryExistingValueFilteringWithSkipLimitAfterLengthReturnsEmpty()
        {
            // Arrange            
            var value1 = CreateValue();
            var value2 = CreateValue();
            var value3 = CreateValue();
            var value4 = CreateValue();
            var value5 = CreateValue();
            var cancellationToken = CancellationToken.None;
            var skip = 3;
            var take = 2;
            var storage = await CreateAsync(value1, value2, value3, value4, value5);
            var filter = new[] { value1.GuidProperty, value2.GuidProperty, value3.GuidProperty }.GetContainsExpressionForGuidProperty();

            // Act
            var actual = await storage.QueryAsync<Item>(filter, null, skip, take, cancellationToken);

            // Assert            
            var actualList = await actual.ToListAsync();
            AssertEquals(actualList.Count, 0);
            AssertEquals(actual.Total, 3);
        }
    }
}
