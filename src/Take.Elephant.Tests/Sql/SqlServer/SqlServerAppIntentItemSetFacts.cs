using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Take.Elephant.Sql;
using Take.Elephant.Sql.Mapping;
using Xunit;

namespace Take.Elephant.Tests.Sql.SqlServer
{
    [Collection(nameof(SqlServer)), Trait("Category", nameof(SqlServer))]
    public class SqlServerAppIntentItemSetFacts : SqlItemSetFacts
    {
        private readonly SqlServerFixture _serverFixture;
        private readonly AuditableDatabaseDriver _databaseDriver;

        public SqlServerAppIntentItemSetFacts(SqlServerFixture serverFixture)
            : base(serverFixture)
        {
            _serverFixture = serverFixture;
            _databaseDriver = new AuditableDatabaseDriver(serverFixture.DatabaseDriver);
        }        
        
        protected override ISet<Item> Create(ITable table, TypeMapper<Item> mapper) => new ApplicationIntentSqlSet<Item>(_databaseDriver, _serverFixture.ConnectionString, table, mapper);
    }
}
