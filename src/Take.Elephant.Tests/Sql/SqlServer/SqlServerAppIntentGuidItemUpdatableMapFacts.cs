using System;
using System.Threading.Tasks;
using Shouldly;
using Take.Elephant.Sql;
using Take.Elephant.Sql.Mapping;
using Xunit;

namespace Take.Elephant.Tests.Sql.SqlServer
{
    [Collection(nameof(SqlServer)), Trait("Category", nameof(SqlServer))]
    public class SqlServerAppIntentGuidItemUpdatableMapFacts : SqlGuidItemUpdatableMapFacts
    {
        private readonly SqlServerFixture _serverFixture;
        private readonly AuditableDatabaseDriver _databaseDriver;

        public SqlServerAppIntentGuidItemUpdatableMapFacts(SqlServerFixture serverFixture)
            : base(serverFixture)
        {
            _serverFixture = serverFixture;
            _databaseDriver = (AuditableDatabaseDriver)serverFixture.DatabaseDriver;
            _databaseDriver.ReceivedConnectionStrings.Clear();
        }

        protected override IUpdatableMap<Guid, Item> Create(ITable table, ValueMapper<Guid> keyMapper, TypeMapper<Item> valueMapper) => 
            new ApplicationIntentSqlMap<Guid, Item>(_serverFixture.DatabaseDriver, _serverFixture.ConnectionString, table, keyMapper, valueMapper);

        [Fact]
        public override async Task UpdateExistingValueSucceeds()
        {
            await base.UpdateExistingValueSucceeds();
            
            _databaseDriver.ReceivedConnectionStrings.Count.ShouldBe(4);
            _databaseDriver.ReceivedConnectionStrings[0].ShouldNotContain("ApplicationIntent"); // Schema synchronization
            _databaseDriver.ReceivedConnectionStrings[1].ShouldNotContain("ApplicationIntent"); // TryAddAsync operation
            _databaseDriver.ReceivedConnectionStrings[2].ShouldNotContain("ApplicationIntent"); // TryUpdateAsync operation
            _databaseDriver.ReceivedConnectionStrings[3].ShouldContain("ApplicationIntent=ReadOnly"); // GetPropertyValueOrDefaultAsync operation
        }
    }
}
