using System;
using System.Threading.Tasks;
using Shouldly;
using Take.Elephant.Sql;
using Take.Elephant.Sql.Mapping;
using Xunit;

namespace Take.Elephant.Tests.Sql.SqlServer
{
    [Collection(nameof(SqlServer)), Trait("Category", nameof(SqlServer))]
    public class SqlServerAppIntentGuidItemPropertyMapFacts : SqlGuidItemPropertyMapFacts
    {
        private readonly SqlServerFixture _serverFixture;
        private readonly AuditableDatabaseDriver _databaseDriver;

        public SqlServerAppIntentGuidItemPropertyMapFacts(SqlServerFixture serverFixture)
            : base(serverFixture)
        {
            _serverFixture = serverFixture;
            _databaseDriver = new AuditableDatabaseDriver(serverFixture.DatabaseDriver);
        }
        
        protected override IPropertyMap<Guid, Item> Create(ITable table, ValueMapper<Guid> keyMapper,
            TypeMapper<Item> valueMapper) =>
            new ApplicationIntentSqlMap<Guid, Item>(_databaseDriver, _serverFixture.ConnectionString,
                table, keyMapper, valueMapper);

        [Fact]
        public override async Task SetPropertyOfExistingKeySucceeds()
        {
            await base.SetPropertyOfExistingKeySucceeds();

            _databaseDriver.ReceivedConnectionStrings.Count.ShouldBe(4);
            _databaseDriver.ReceivedConnectionStrings[0].ShouldNotContain("ApplicationIntent"); // Schema synchronization
            _databaseDriver.ReceivedConnectionStrings[1].ShouldNotContain("ApplicationIntent"); // TryAddAsync operation
            _databaseDriver.ReceivedConnectionStrings[2].ShouldNotContain("ApplicationIntent"); // SetPropertyValueAsync operation
            _databaseDriver.ReceivedConnectionStrings[3].ShouldContain("ApplicationIntent=ReadOnly"); // GetPropertyValueOrDefaultAsync operation
        }

        [Fact]
        public override async Task MergeWithExistingValueSucceeds()
        {
            await base.MergeWithExistingValueSucceeds();

            _databaseDriver.ReceivedConnectionStrings.Count.ShouldBe(4);
            _databaseDriver.ReceivedConnectionStrings[0].ShouldNotContain("ApplicationIntent"); // Schema synchronization
            _databaseDriver.ReceivedConnectionStrings[1].ShouldNotContain("ApplicationIntent"); // TryAddAsync operation
            _databaseDriver.ReceivedConnectionStrings[2].ShouldNotContain("ApplicationIntent"); // MergeAsync operation
            _databaseDriver.ReceivedConnectionStrings[3].ShouldContain("ApplicationIntent=ReadOnly"); // GetPropertyValueOrDefaultAsync operation
        }
    }
}