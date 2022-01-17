using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Shouldly;
using Take.Elephant.Sql;
using Take.Elephant.Sql.Mapping;
using Xunit;

namespace Take.Elephant.Tests.Sql.SqlServer
{
    [Collection(nameof(SqlServer)), Trait("Category", nameof(SqlServer))]
    public class SqlServerAppIntentGuidItemSetMapFacts : SqlGuidItemSetMapFacts
    {
        private readonly SqlServerFixture _serverFixture;
        private readonly AuditableDatabaseDriver _databaseDriver;

        public SqlServerAppIntentGuidItemSetMapFacts(SqlServerFixture serverFixture)
            : base(serverFixture)
        {
            _serverFixture = serverFixture;
            _databaseDriver = new AuditableDatabaseDriver(serverFixture.DatabaseDriver);
        }

        protected override IMap<Guid, ISet<Item>> Create(Table table, ValueMapper<Guid> keyMapper, TypeMapper<Item> valueMapper) => 
            new ApplicationIntentSqlSetMap<Guid, Item>(_databaseDriver, _serverFixture.ConnectionString, table, keyMapper, valueMapper);

        [Fact]
        public override async Task AddNewKeyAndValueSucceeds()
        {
            await base.AddNewKeyAndValueSucceeds();
            
            _databaseDriver.ReceivedConnectionStrings.Count.ShouldBe(4);
            _databaseDriver.ReceivedConnectionStrings[0].ShouldNotContain("ApplicationIntent"); // Schema synchronization
            _databaseDriver.ReceivedConnectionStrings[1].ShouldNotContain("ApplicationIntent"); // TryAddAsync operation
            // The operations below should be, ideally, executed in a ReadOnly connection, but the current ApplicationIntentSqlSetMap implementation doesn't support it.
            // The reason is that the underlying SqlSetMap returns an InternalSqlSetMap which cannot be intercepted to change the connection string.
            _databaseDriver.ReceivedConnectionStrings[2].ShouldNotContain("ApplicationIntent"); // GetValueOrDefaultAsync/Contains operation
            _databaseDriver.ReceivedConnectionStrings[3].ShouldNotContain("ApplicationIntent"); // ISet.AsEnumerableAsync operation
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

        [Fact]
        public override async Task GetEmptyListSucceeds()
        {
            await base.GetEmptyListSucceeds();
            _databaseDriver.ReceivedConnectionStrings.Count.ShouldBe(2);
            _databaseDriver.ReceivedConnectionStrings[0].ShouldNotContain("ApplicationIntent"); // Schema synchronization
            // The operation below should be, ideally, executed in a ReadOnly connection, but the current ApplicationIntentSqlSetMap implementation doesn't support it.
            // The reason is that the underlying SqlSetMap returns an InternalSqlSetMap which cannot be intercepted to change the connection string.            
            _databaseDriver.ReceivedConnectionStrings[1].ShouldNotContain("ApplicationIntent"); // ISet.GetLenghtAsync operation
        }
    }
}
