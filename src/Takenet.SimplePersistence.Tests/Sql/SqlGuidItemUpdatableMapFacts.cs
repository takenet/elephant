using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Takenet.SimplePersistence.Sql;
using Takenet.SimplePersistence.Sql.Mapping;
using Xunit;

namespace Takenet.SimplePersistence.Tests.Sql
{
    [Collection("Sql")]
    public class SqlGuidItemUpdatableMapFacts : GuidItemUpdatableMapFacts
    {
        private readonly SqlConnectionFixture _fixture;

        public SqlGuidItemUpdatableMapFacts(SqlConnectionFixture fixture)
        {
            _fixture = fixture;
        }

        public override IUpdatableMap<Guid, Item> Create()
        {
            var databaseDriver = new SqlDatabaseDriver();
            var table = TableBuilder
                .WithName("GuidItems")
                .WithColumnsFromTypeProperties<Item>()
                .WithKeyColumnFromType<Guid>("Key")
                .Build();
            _fixture.DropTable(table.Name);

            var keyMapper = new ValueMapper<Guid>("Key");
            var valueMapper = new TypeMapper<Item>(table);
            return new SqlMap<Guid, Item>(databaseDriver, _fixture.ConnectionString, table, keyMapper, valueMapper);
        }
    }
}
