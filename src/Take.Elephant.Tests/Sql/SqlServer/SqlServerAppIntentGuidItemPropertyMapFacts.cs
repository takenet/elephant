using System;
using System.Threading.Tasks;
using Shouldly;
using Take.Elephant.Sql;
using Take.Elephant.Sql.Mapping;
using Xunit;

namespace Take.Elephant.Tests.Sql.SqlServer
{
    [Collection(nameof(SqlServer)), Trait("Category", nameof(SqlServer))]
    public class SqlServerAppIntentGuidItemPropertyMapFacts : GuidItemPropertyMapFacts
    {
        private readonly SqlServerFixture _serverFixture;
        private readonly AuditableDatabaseDriver _databaseDriver;

        public SqlServerAppIntentGuidItemPropertyMapFacts(SqlServerFixture serverFixture)
        {
            _serverFixture = serverFixture;
            _databaseDriver = (AuditableDatabaseDriver)serverFixture.DatabaseDriver;
            _databaseDriver.ReceivedConnectionStrings.Clear();
        }

        public override IPropertyMap<Guid, Item> Create()
        {
            var table = TableBuilder
                .WithName("GuidItems")
                .WithColumnsFromTypeProperties<Item>()
                .WithKeyColumnFromType<Guid>("Key")
                .Build();
            _serverFixture.DropTable(table.Schema, table.Name);

            var keyMapper = new ValueMapper<Guid>("Key");
            var valueMapper = new TypeMapper<Item>(table);
            return new ApplicationIntentSqlMap<Guid, Item>(_serverFixture.DatabaseDriver, _serverFixture.ConnectionString, table, keyMapper, valueMapper);
        }
        
        [Fact]
        public override async Task SetPropertyOfExistingKeySucceeds()
        {
            await base.SetPropertyOfExistingKeySucceeds();
            
            _databaseDriver.ReceivedConnectionStrings.Count.ShouldBe(4);
            _databaseDriver.ReceivedConnectionStrings[0].ShouldNotContain("ApplicationIntent");       // Schema synchronization
            _databaseDriver.ReceivedConnectionStrings[1].ShouldNotContain("ApplicationIntent");       // TryAddAsync operation
            _databaseDriver.ReceivedConnectionStrings[2].ShouldNotContain("ApplicationIntent");       // SetPropertyValueAsync operation
            _databaseDriver.ReceivedConnectionStrings[3].ShouldContain("ApplicationIntent=ReadOnly"); // GetPropertyValueOrDefaultAsync operation
        }

        [Fact]
        public override async Task MergeWithExistingValueSucceeds()
        {
            await base.MergeWithExistingValueSucceeds();
            
            _databaseDriver.ReceivedConnectionStrings.Count.ShouldBe(4);
            _databaseDriver.ReceivedConnectionStrings[0].ShouldNotContain("ApplicationIntent");       // Schema synchronization
            _databaseDriver.ReceivedConnectionStrings[1].ShouldNotContain("ApplicationIntent");       // TryAddAsync operation
            _databaseDriver.ReceivedConnectionStrings[2].ShouldNotContain("ApplicationIntent");       // MergeAsync operation
            _databaseDriver.ReceivedConnectionStrings[3].ShouldContain("ApplicationIntent=ReadOnly"); // GetPropertyValueOrDefaultAsync operation
        }
    }
}
