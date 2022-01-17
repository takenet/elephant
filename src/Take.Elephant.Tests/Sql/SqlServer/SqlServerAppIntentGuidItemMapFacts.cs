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
    public class SqlServerAppIntentGuidItemMapFacts : SqlGuidItemMapFacts
    {
        private readonly SqlServerFixture _serverFixture;
        private readonly AuditableDatabaseDriver _databaseDriver;

        public SqlServerAppIntentGuidItemMapFacts(SqlServerFixture serverFixture)
            : base(serverFixture)
        {
            _serverFixture = serverFixture;
            _databaseDriver = new AuditableDatabaseDriver(serverFixture.DatabaseDriver);
        }

        protected override IMap<Guid, Item> Create(ITable table, ValueMapper<Guid> keyMapper, TypeMapper<Item> valueMapper) =>
            new ApplicationIntentSqlMap<Guid, Item>(_databaseDriver, _serverFixture.ConnectionString, table, keyMapper, valueMapper);

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
            _databaseDriver.ReceivedConnectionStrings[1].ShouldNotContain("ApplicationIntent");       // TryAddAsync operation
            _databaseDriver.ReceivedConnectionStrings[2].ShouldContain("ApplicationIntent=ReadOnly"); // ContainsKeyAsync operation
        }
    }
}
