using System;
using Takenet.Elephant.Sql;
using Takenet.Elephant.Sql.Mapping;
using Xunit;

namespace Takenet.Elephant.Tests.Sql
{
    [Collection("Sql")]
    public class SqlGuidItemUpdatableMapFacts : GuidItemUpdatableMapFacts
    {
        private readonly SqlFixture _fixture;

        public SqlGuidItemUpdatableMapFacts(SqlFixture fixture)
        {
            _fixture = fixture;
        }

        public override IUpdatableMap<Guid, Item> Create()
        {
            var table = TableBuilder
                .WithName("GuidItems")
                .WithColumnsFromTypeProperties<Item>()
                .WithKeyColumnFromType<Guid>("Key")
                .Build();
            _fixture.DropTable(table.Name);

            var keyMapper = new ValueMapper<Guid>("Key");
            var valueMapper = new TypeMapper<Item>(table);
            return new SqlMap<Guid, Item>(_fixture.DatabaseDriver, _fixture.ConnectionString, table, keyMapper, valueMapper);
        }
    }
}
