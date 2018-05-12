using System;
using Take.Elephant.Sql;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Tests.Sql
{
    public abstract class SqlGuidItemPropertyMapFacts : GuidItemPropertyMapFacts
    {
        private readonly ISqlFixture _serverFixture;

        protected SqlGuidItemPropertyMapFacts(ISqlFixture serverFixture)
        {
            _serverFixture = serverFixture;
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
            return new SqlMap<Guid, Item>(_serverFixture.DatabaseDriver, _serverFixture.ConnectionString, table, keyMapper, valueMapper);
        }
    }
}
