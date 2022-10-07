using System;
using Take.Elephant.Sql;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Tests.Sql
{
    public abstract class SqlGuidNumberMapFacts : GuidItemNumberMapFacts
    {
        private readonly ISqlFixture _serverFixture;

        protected SqlGuidNumberMapFacts(ISqlFixture serverFixture)
        {
            _serverFixture = serverFixture;
        }

        public override INumberMap<Guid> Create()
        {
            var table = TableBuilder
                .WithName("GuidNumbers")
                .WithKeyColumnFromType<Guid>("Key")
                .WithColumnFromType<long>("Counter")
                .WithSynchronizationStrategy(SchemaSynchronizationStrategy.UntilSuccess)
                .Build();
            _serverFixture.DropTable(table.Schema, table.Name);
            var keyMapper = new ValueMapper<Guid>("Key");
            return new SqlNumberMap<Guid>(_serverFixture.DatabaseDriver, _serverFixture.ConnectionString, table, keyMapper, "Counter");
        }
    }
}
