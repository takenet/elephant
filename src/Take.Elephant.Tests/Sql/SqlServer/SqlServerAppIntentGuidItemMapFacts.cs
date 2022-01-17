using System;
using System.Data;
using System.Threading.Tasks;
using Shouldly;
using Take.Elephant.Sql;
using Take.Elephant.Sql.Mapping;
using Xunit;

namespace Take.Elephant.Tests.Sql.SqlServer
{
    [Collection(nameof(SqlServer)), Trait("Category", nameof(SqlServer))]
    public class SqlServerAppIntentGuidItemMapFacts : GuidItemMapFacts
    {
        private readonly SqlServerFixture _serverFixture;
        private readonly AuditableDatabaseDriver _databaseDriver;

        public SqlServerAppIntentGuidItemMapFacts(SqlServerFixture serverFixture)
        {
            _serverFixture = serverFixture;
            _databaseDriver = (AuditableDatabaseDriver)serverFixture.DatabaseDriver;
        }
        
        public override IMap<Guid, Item> Create()
        {
            var table = TableBuilder
                .WithName("GuidItems")
                .WithColumnsFromTypeProperties<Item>(p => !p.Name.Equals(nameof(Item.StringProperty)))
                .WithColumn(nameof(Item.StringProperty), new SqlType(DbType.String, int.MaxValue))
                .WithKeyColumnFromType<Guid>("Key")
                .Build();
            _serverFixture.DropTable(table.Schema, table.Name);

            var keyMapper = new ValueMapper<Guid>("Key");
            var valueMapper = new TypeMapper<Item>(table);
            return new ApplicationIntentSqlMap<Guid, Item>(_serverFixture.DatabaseDriver, _serverFixture.ConnectionString, table, keyMapper, valueMapper);
        }

        [Fact]
        public override async Task AddNewKeyAndValueSucceeds()
        {
            await base.AddNewKeyAndValueSucceeds();
            _databaseDriver.ReceivedConnectionStrings.Count.ShouldBe(3);
            _databaseDriver.ReceivedConnectionStrings[0].ShouldNotContain("ApplicationIntent");       // Schema synchronization
            _databaseDriver.ReceivedConnectionStrings[1].ShouldNotContain("ApplicationIntent");       // TryAddAsync operation
            _databaseDriver.ReceivedConnectionStrings[2].ShouldContain("ApplicationIntent=ReadOnly"); // GetValueOrDefaultAsync operation
        }

        [Fact]
        public override async Task TryRemoveNonExistingKeyFails()
        {
            await base.TryRemoveNonExistingKeyFails();
            _databaseDriver.ReceivedConnectionStrings.Count.ShouldBe(2);
            _databaseDriver.ReceivedConnectionStrings[0].ShouldNotContain("ApplicationIntent"); // Schema synchronization
            _databaseDriver.ReceivedConnectionStrings[1].ShouldNotContain("ApplicationIntent"); // TryRemoveAsync operation
        }

        [Fact]
        public override async Task CheckForExistingKeyReturnsTrue()
        {
            await base.CheckForExistingKeyReturnsTrue();
            _databaseDriver.ReceivedConnectionStrings.Count.ShouldBe(3);
            _databaseDriver.ReceivedConnectionStrings[0].ShouldNotContain("ApplicationIntent");       // Schema synchronization
            _databaseDriver.ReceivedConnectionStrings[1].ShouldNotContain("ApplicationIntent");       // TryAdd operation
            _databaseDriver.ReceivedConnectionStrings[2].ShouldContain("ApplicationIntent=ReadOnly"); // ContainsKeyAsync operation
        }
    }
}
