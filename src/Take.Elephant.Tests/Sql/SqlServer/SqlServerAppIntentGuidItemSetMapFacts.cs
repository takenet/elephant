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
            _databaseDriver.ReceivedConnectionStrings[0].ShouldNotContain("ApplicationIntent");       // Schema synchronization
            _databaseDriver.ReceivedConnectionStrings[1].ShouldNotContain("ApplicationIntent");       // TryAddAsync operation
            _databaseDriver.ReceivedConnectionStrings[2].ShouldContain("ApplicationIntent=ReadOnly"); // GetValueOrDefaultAsync (contains) operation
            _databaseDriver.ReceivedConnectionStrings[3].ShouldContain("ApplicationIntent=ReadOnly"); // AsEnumerableAsync operation
        }

    }
}
